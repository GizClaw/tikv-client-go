// Copyright 2026 TiKV Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0

package tikv

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/pingcap/kvproto/pkg/errorpb"
	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/stretchr/testify/require"
	"github.com/tikv/client-go/v2/internal/mockstore/mocktikv"
	"github.com/tikv/client-go/v2/kv"
	"github.com/tikv/client-go/v2/testutils"
	"github.com/tikv/client-go/v2/tikvrpc"
	"github.com/tikv/client-go/v2/util/async"
)

type snapshotLabelClient struct {
	Client
	mu        sync.Mutex
	stores    []uint64
	retryScan bool
}

func (c *snapshotLabelClient) SendRequest(ctx context.Context, addr string, req *tikvrpc.Request, timeout time.Duration) (*tikvrpc.Response, error) {
	if req.Type == tikvrpc.CmdGet || req.Type == tikvrpc.CmdBatchGet || req.Type == tikvrpc.CmdScan {
		c.mu.Lock()
		c.stores = append(c.stores, req.Peer.GetStoreId())
		retryScan := c.retryScan && req.Type == tikvrpc.CmdScan
		if retryScan {
			c.retryScan = false
		}
		c.mu.Unlock()
		if retryScan {
			return &tikvrpc.Response{Resp: &kvrpcpb.ScanResponse{RegionError: &errorpb.Error{EpochNotMatch: &errorpb.EpochNotMatch{}}}}, nil
		}
		// mocktikv rejects all nonleader requests. Supply normal read replies
		// here after the real RegionCache/selector has chosen the destination.
		switch req.Type {
		case tikvrpc.CmdGet:
			return &tikvrpc.Response{Resp: &kvrpcpb.GetResponse{Value: []byte("one")}}, nil
		case tikvrpc.CmdBatchGet:
			return &tikvrpc.Response{Resp: &kvrpcpb.BatchGetResponse{Pairs: []*kvrpcpb.KvPair{{Key: []byte("a"), Value: []byte("one")}, {Key: []byte("b"), Value: []byte("two")}}}}, nil
		case tikvrpc.CmdScan:
			pairs := []*kvrpcpb.KvPair{{Key: []byte("a"), Value: []byte("one")}, {Key: []byte("b"), Value: []byte("two")}}
			if req.Scan().Reverse {
				pairs[0], pairs[1] = pairs[1], pairs[0]
			}
			return &tikvrpc.Response{Resp: &kvrpcpb.ScanResponse{Pairs: pairs}}, nil
		}
	}
	return c.Client.SendRequest(ctx, addr, req, timeout)
}

func (c *snapshotLabelClient) SendRequestAsync(ctx context.Context, addr string, req *tikvrpc.Request, cb async.Callback[*tikvrpc.Response]) {
	resp, err := c.SendRequest(ctx, addr, req, time.Second)
	cb.Schedule(resp, err)
}

// This exercises the actual snapshot, RegionCache and replica selector over a
// mock topology with two labelled Learners and successful read replies. Repetition bounds the negative
// control's random selection: an implementation dropping labels has probability
// at most 2^-32 of choosing the expected Learner in all 32 independent scans.
func TestSnapshotReadStoreLabels(t *testing.T) {
	for _, operation := range []string{"Get", "BatchGet", "Scan", "ReverseScan"} {
		for _, label := range []string{"a", "b", ""} {
			t.Run(fmt.Sprintf("%s/label=%s", operation, label), func(t *testing.T) {
				client, cluster, pdClient, err := testutils.NewMockTiKV("", nil)
				require.NoError(t, err)
				leader, _, regionID := mocktikv.BootstrapWithSingleStore(cluster)
				learners := map[string]uint64{}
				for _, name := range []string{"a", "b"} {
					id, peer := cluster.AllocID(), cluster.AllocID()
					cluster.AddStore(id, "learner-"+name, &metapb.StoreLabel{Key: "region", Value: name})
					cluster.AddLearner(regionID, id, peer)
					learners[name] = id
				}
				var observer *snapshotLabelClient
				store, err := NewTestTiKVStore(client, pdClient, func(base Client) Client { observer = &snapshotLabelClient{Client: base}; return observer }, nil, 0)
				require.NoError(t, err)
				defer store.Close()
				txn, err := store.Begin()
				require.NoError(t, err)
				require.NoError(t, txn.Set([]byte("a"), []byte("one")))
				require.NoError(t, txn.Set([]byte("b"), []byte("two")))
				require.NoError(t, txn.Commit(context.Background()))
				txn, err = store.Begin()
				require.NoError(t, err)
				defer txn.Rollback()
				bo := NewBackofferWithVars(context.Background(), 5000, nil)
				loc, err := store.GetRegionCache().LocateKey(bo, []byte("a"))
				require.NoError(t, err)
				for attempt := 0; attempt < 32; attempt++ {
					// Alternate a cold Region cache with reuse, and force one
					// region-epoch retry for each scan in the second half.
					if attempt%2 == 0 {
						store.GetRegionCache().InvalidateCachedRegion(loc.Region)
					}
					observer.mu.Lock()
					observer.stores = nil
					observer.retryScan = attempt >= 16 && (operation == "Scan" || operation == "ReverseScan")
					observer.mu.Unlock()
					snapshot := store.GetSnapshot(txn.StartTS())
					snapshot.SetReplicaRead(kv.ReplicaReadLearner)
					if label != "" {
						snapshot.SetMatchStoreLabels([]*metapb.StoreLabel{{Key: "region", Value: label}})
					}
					switch operation {
					case "Get":
						value, err := snapshot.Get(context.Background(), []byte("a"))
						require.NoError(t, err)
						require.Equal(t, []byte("one"), value.Value)
					case "BatchGet":
						values, err := snapshot.BatchGet(context.Background(), [][]byte{[]byte("a"), []byte("b")})
						require.NoError(t, err)
						require.Equal(t, []byte("one"), values["a"].Value)
						require.Equal(t, []byte("two"), values["b"].Value)
					default:
						var iterator Iterator
						var err error
						if operation == "Scan" {
							iterator, err = snapshot.Iter([]byte("a"), []byte("c"))
						} else {
							iterator, err = snapshot.IterReverse([]byte("c"), []byte("a"))
						}
						require.NoError(t, err)
						var keys []string
						for iterator.Valid() {
							keys = append(keys, string(iterator.Key()))
							require.NoError(t, iterator.Next())
						}
						iterator.Close()
						want := []string{"a", "b"}
						if operation == "ReverseScan" {
							want = []string{"b", "a"}
						}
						require.Equal(t, want, keys)
					}
					observer.mu.Lock()
					selected := append([]uint64(nil), observer.stores...)
					observer.mu.Unlock()
					require.NotEmpty(t, selected)
					if attempt >= 16 && (operation == "Scan" || operation == "ReverseScan") {
						require.GreaterOrEqual(t, len(selected), 2, "region retry must actually run")
					}
					for _, id := range selected {
						require.NotEqual(t, leader, id)
						if label != "" {
							require.Equal(t, learners[label], id, "attempt %d selected nonmatching Learner", attempt)
						}
					}
				}
			})
		}
	}
}
