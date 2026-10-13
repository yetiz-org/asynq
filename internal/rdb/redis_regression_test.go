// Copyright 2020 Kentaro Hibino. All rights reserved.
// Use of this source code is governed by a MIT license
// that can be found in the LICENSE file.

package rdb

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/yetiz-org/asynq/internal/base"
	h "github.com/yetiz-org/asynq/internal/testutil"
)

func TestCurrentStatsMemoryUsageMissingTasks(t *testing.T) {
	r := setup(t)
	defer r.Close()
	ctx := context.Background()
	const qname = "missing-samples"
	if err := r.client.SAdd(ctx, base.AllQueuesKey(""), qname).Err(); err != nil {
		t.Fatal(err)
	}
	if err := r.client.LPush(ctx, base.PendingKey("", qname), "missing-list").Err(); err != nil {
		t.Fatal(err)
	}
	if err := r.client.ZAdd(ctx, base.ScheduledKey("", qname), redis.Z{Member: "missing-zset", Score: 1}).Err(); err != nil {
		t.Fatal(err)
	}

	stats, err := r.CurrentStats(qname)
	if err != nil {
		t.Fatalf("CurrentStats with missing task hashes: %v", err)
	}
	listUsage, err := r.client.MemoryUsage(ctx, base.PendingKey("", qname)).Result()
	if err != nil {
		t.Fatal(err)
	}
	zsetUsage, err := r.client.MemoryUsage(ctx, base.ScheduledKey("", qname)).Result()
	if err != nil {
		t.Fatal(err)
	}
	if stats.Pending != 1 || stats.Scheduled != 1 || stats.Size != 2 || stats.MemoryUsage != listUsage+zsetUsage {
		t.Errorf("stats = %+v; want pending=1 scheduled=1 size=2 memory=%d", stats, listUsage+zsetUsage)
	}

	h.FlushDB(t, r.client)
	if err := r.client.SAdd(ctx, base.AllQueuesKey(""), qname).Err(); err != nil {
		t.Fatal(err)
	}
	if err := r.client.LPush(ctx, base.PendingKey("", qname), "existing-list").Err(); err != nil {
		t.Fatal(err)
	}
	if err := r.client.ZAdd(ctx, base.ScheduledKey("", qname), redis.Z{Member: "existing-zset", Score: 1}).Err(); err != nil {
		t.Fatal(err)
	}
	for _, id := range []string{"existing-list", "existing-zset"} {
		if err := r.client.HSet(ctx, base.TaskKey("", qname, id), "state", "scheduled").Err(); err != nil {
			t.Fatal(err)
		}
	}
	stats, err = r.CurrentStats(qname)
	if err != nil {
		t.Fatalf("CurrentStats with existing task hashes: %v", err)
	}
	wantUsage := int64(0)
	for _, key := range []string{
		base.PendingKey("", qname), base.ScheduledKey("", qname),
		base.TaskKey("", qname, "existing-list"), base.TaskKey("", qname, "existing-zset"),
	} {
		usage, err := r.client.MemoryUsage(ctx, key).Result()
		if err != nil {
			t.Fatal(err)
		}
		wantUsage += usage
	}
	if stats.Pending != 1 || stats.Scheduled != 1 || stats.Size != 2 || stats.MemoryUsage != wantUsage {
		t.Errorf("normal stats = %+v; want pending=1 scheduled=1 size=2 memory=%d", stats, wantUsage)
	}
}

func TestCurrentStatsMemoryUsageMissingGroupTask(t *testing.T) {
	r := setup(t)
	defer r.Close()
	ctx := context.Background()
	const qname = "missing-group-sample"
	const gname = "one-group"
	if err := r.client.SAdd(ctx, base.AllQueuesKey(""), qname).Err(); err != nil {
		t.Fatal(err)
	}
	if err := r.client.SAdd(ctx, base.AllGroups("", qname), gname).Err(); err != nil {
		t.Fatal(err)
	}
	if err := r.client.ZAdd(ctx, base.GroupKey("", qname, gname), redis.Z{Member: "missing-group-task", Score: 1}).Err(); err != nil {
		t.Fatal(err)
	}
	stats, err := r.CurrentStats(qname)
	if err != nil {
		t.Fatalf("CurrentStats with missing group task hash: %v", err)
	}
	if stats.Groups != 1 || stats.Aggregating != 1 || stats.Size != 1 || stats.MemoryUsage != 0 {
		t.Errorf("stats = %+v; want groups=1 aggregating=1 size=1 memory=0", stats)
	}
	if err := r.client.HSet(ctx, base.TaskKey("", qname, "missing-group-task"), "state", "aggregating").Err(); err != nil {
		t.Fatal(err)
	}
	stats, err = r.CurrentStats(qname)
	if err != nil {
		t.Fatalf("CurrentStats with existing group task hash: %v", err)
	}
	wantUsage, err := r.client.MemoryUsage(ctx, base.TaskKey("", qname, "missing-group-task")).Result()
	if err != nil {
		t.Fatal(err)
	}
	if stats.Groups != 1 || stats.Aggregating != 1 || stats.Size != 1 || stats.MemoryUsage != wantUsage {
		t.Errorf("normal group stats = %+v; want groups=1 aggregating=1 size=1 memory=%d", stats, wantUsage)
	}
}

func TestCancelationPubSubClosesAfterReceiveError(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	observed := make(chan error, 1)
	go func() {
		conn, err := listener.Accept()
		if err != nil {
			observed <- err
			return
		}
		defer conn.Close()
		if err := conn.SetDeadline(time.Now().Add(3 * time.Second)); err != nil {
			observed <- err
			return
		}
		reader := bufio.NewReader(conn)
		if cmd, err := readRESPCommand(reader); err != nil || cmd != "hello" {
			observed <- fmt.Errorf("initial command = %q, %v; want hello", cmd, err)
			return
		}
		if _, err := conn.Write([]byte("-ERR unsupported HELLO\r\n")); err != nil {
			observed <- err
			return
		}
		if cmd, err := readRESPCommand(reader); err != nil || cmd != "subscribe" {
			observed <- fmt.Errorf("subscription command = %q, %v; want subscribe", cmd, err)
			return
		}
		if _, err := conn.Write([]byte("-ERR forced subscribe failure\r\n")); err != nil {
			observed <- err
			return
		}
		if _, err := reader.ReadByte(); !errors.Is(err, io.EOF) {
			observed <- fmt.Errorf("socket after Receive error: %v; want EOF from closed PubSub", err)
			return
		}
		observed <- nil
	}()
	client := redis.NewClient(&redis.Options{Addr: listener.Addr().String(), Protocol: 2, DisableIndentity: true, MaxRetries: -1})
	r := NewRDB(client)
	defer r.Close()
	pubsub, err := r.CancelationPubSub()
	if pubsub != nil || err == nil {
		t.Errorf("CancelationPubSub = %v, %v; want nil and error", pubsub, err)
	}
	select {
	case err := <-observed:
		if err != nil {
			t.Error(err)
		}
	case <-time.After(3 * time.Second):
		t.Error("server did not observe PubSub socket close")
	}
}

func readRESPCommand(reader *bufio.Reader) (cmd string, rtErr error) {
	var count int
	if _, err := fmt.Fscanf(reader, "*%d\r\n", &count); err != nil {
		return "", err
	}
	for i := range count {
		var size int
		if _, err := fmt.Fscanf(reader, "$%d\r\n", &size); err != nil {
			return "", err
		}
		data := make([]byte, size+2)
		if _, err := io.ReadFull(reader, data); err != nil {
			return "", err
		}
		if i == 0 {
			cmd = strings.ToLower(string(data[:size]))
		}
	}
	return cmd, nil
}
