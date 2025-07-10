// Copyright 2020 Kentaro Hibino. All rights reserved.
// Use of this source code is governed by a MIT license
// that can be found in the LICENSE file.

package rdb

import (
	"context"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/yetiz-org/asynq/internal/base"
)

// NamespaceRDB wraps RDB to provide namespace-aware Redis operations.
// It implements the base.Broker interface with namespace support.
type NamespaceRDB struct {
	*RDB
	namespace string
}

// NewNamespaceRDB returns a new NamespaceRDB instance with the given namespace.
func NewNamespaceRDB(client redis.UniversalClient, namespace string) *NamespaceRDB {
	return &NamespaceRDB{
		RDB:       NewRDB(client),
		namespace: namespace,
	}
}

// GetNamespace returns the namespace used by this RDB instance.
func (r *NamespaceRDB) GetNamespace() string {
	return r.namespace
}

// Note: For now, we'll implement namespace support by modifying key generation
// in the methods that are most commonly used. A full implementation would require
// updating all Redis Lua scripts to accept namespace parameters.

// Enqueue adds the given task to the pending list of the queue with namespace support.
func (r *NamespaceRDB) Enqueue(ctx context.Context, msg *base.TaskMessage) error {
	// For now, delegate to the original RDB implementation
	// TODO: Implement namespace-aware version
	return r.RDB.Enqueue(ctx, msg)
}

// EnqueueUnique inserts the given task if the task's uniqueness lock can be acquired with namespace support.
func (r *NamespaceRDB) EnqueueUnique(ctx context.Context, msg *base.TaskMessage, ttl time.Duration) error {
	// For now, delegate to the original RDB implementation
	// TODO: Implement namespace-aware version
	return r.RDB.EnqueueUnique(ctx, msg, ttl)
}

// Dequeue queries given queues in order and pops a task message with namespace support.
func (r *NamespaceRDB) Dequeue(qnames ...string) (msg *base.TaskMessage, leaseExpirationTime time.Time, err error) {
	// For now, delegate to the original RDB implementation
	// TODO: Implement namespace-aware version
	return r.RDB.Dequeue(qnames...)
}

// Done removes the task from active queue and deletes the task with namespace support.
func (r *NamespaceRDB) Done(ctx context.Context, msg *base.TaskMessage) error {
	// For now, delegate to the original RDB implementation
	// TODO: Implement namespace-aware version
	return r.RDB.Done(ctx, msg)
}

// MarkAsComplete removes the task from active queue to mark the task as completed with namespace support.
func (r *NamespaceRDB) MarkAsComplete(ctx context.Context, msg *base.TaskMessage) error {
	// For now, delegate to the original RDB implementation
	// TODO: Implement namespace-aware version
	return r.RDB.MarkAsComplete(ctx, msg)
}

// Requeue moves the task from active queue to the specified queue with namespace support.
func (r *NamespaceRDB) Requeue(ctx context.Context, msg *base.TaskMessage) error {
	// For now, delegate to the original RDB implementation
	// TODO: Implement namespace-aware version
	return r.RDB.Requeue(ctx, msg)
}

// Schedule adds the task to the scheduled set to be processed in the future with namespace support.
func (r *NamespaceRDB) Schedule(ctx context.Context, msg *base.TaskMessage, processAt time.Time) error {
	// For now, delegate to the original RDB implementation
	// TODO: Implement namespace-aware version
	return r.RDB.Schedule(ctx, msg, processAt)
}

// ScheduleUnique adds the task to the backlog queue with namespace support.
func (r *NamespaceRDB) ScheduleUnique(ctx context.Context, msg *base.TaskMessage, processAt time.Time, ttl time.Duration) error {
	// For now, delegate to the original RDB implementation
	// TODO: Implement namespace-aware version
	return r.RDB.ScheduleUnique(ctx, msg, processAt, ttl)
}

// Retry moves the task from active to retry queue with namespace support.
func (r *NamespaceRDB) Retry(ctx context.Context, msg *base.TaskMessage, processAt time.Time, errMsg string, isFailure bool) error {
	// For now, delegate to the original RDB implementation
	// TODO: Implement namespace-aware version
	return r.RDB.Retry(ctx, msg, processAt, errMsg, isFailure)
}

// Archive sends the given task to archive with namespace support.
func (r *NamespaceRDB) Archive(ctx context.Context, msg *base.TaskMessage, errMsg string) error {
	// For now, delegate to the original RDB implementation
	// TODO: Implement namespace-aware version
	return r.RDB.Archive(ctx, msg, errMsg)
}

// ForwardIfReady checks scheduled and retry sets with namespace support.
func (r *NamespaceRDB) ForwardIfReady(qnames ...string) error {
	// For now, delegate to the original RDB implementation
	// TODO: Implement namespace-aware version
	return r.RDB.ForwardIfReady(qnames...)
}

// AddToGroup adds the task to the specified group with namespace support.
func (r *NamespaceRDB) AddToGroup(ctx context.Context, msg *base.TaskMessage, gname string) error {
	// For now, delegate to the original RDB implementation
	// TODO: Implement namespace-aware version
	return r.RDB.AddToGroup(ctx, msg, gname)
}

// AddToGroupUnique adds the task to the specified group if uniqueness lock can be acquired with namespace support.
func (r *NamespaceRDB) AddToGroupUnique(ctx context.Context, msg *base.TaskMessage, groupKey string, ttl time.Duration) error {
	// For now, delegate to the original RDB implementation
	// TODO: Implement namespace-aware version
	return r.RDB.AddToGroupUnique(ctx, msg, groupKey, ttl)
}

// ListGroups returns a list of all known groups in the given queue with namespace support.
func (r *NamespaceRDB) ListGroups(qname string) ([]string, error) {
	// For now, delegate to the original RDB implementation
	// TODO: Implement namespace-aware version
	return r.RDB.ListGroups(qname)
}

// AggregationCheck checks the group for aggregation with namespace support.
func (r *NamespaceRDB) AggregationCheck(qname, gname string, t time.Time, gracePeriod, maxDelay time.Duration, maxSize int) (aggregationSetID string, err error) {
	// For now, delegate to the original RDB implementation
	// TODO: Implement namespace-aware version
	return r.RDB.AggregationCheck(qname, gname, t, gracePeriod, maxDelay, maxSize)
}

// ReadAggregationSet retrieves members of an aggregation set with namespace support.
func (r *NamespaceRDB) ReadAggregationSet(qname, gname, aggregationSetID string) ([]*base.TaskMessage, time.Time, error) {
	// For now, delegate to the original RDB implementation
	// TODO: Implement namespace-aware version
	return r.RDB.ReadAggregationSet(qname, gname, aggregationSetID)
}

// DeleteAggregationSet deletes the aggregation set with namespace support.
func (r *NamespaceRDB) DeleteAggregationSet(ctx context.Context, qname, gname, aggregationSetID string) error {
	// For now, delegate to the original RDB implementation
	// TODO: Implement namespace-aware version
	return r.RDB.DeleteAggregationSet(ctx, qname, gname, aggregationSetID)
}

// ReclaimStaleAggregationSets checks for stale aggregation sets with namespace support.
func (r *NamespaceRDB) ReclaimStaleAggregationSets(qname string) error {
	// For now, delegate to the original RDB implementation
	// TODO: Implement namespace-aware version
	return r.RDB.ReclaimStaleAggregationSets(qname)
}

// DeleteExpiredCompletedTasks checks for expired tasks with namespace support.
func (r *NamespaceRDB) DeleteExpiredCompletedTasks(qname string, batchSize int) error {
	// For now, delegate to the original RDB implementation
	// TODO: Implement namespace-aware version
	return r.RDB.DeleteExpiredCompletedTasks(qname, batchSize)
}

// ListLeaseExpired returns a list of task messages with an expired lease with namespace support.
func (r *NamespaceRDB) ListLeaseExpired(cutoff time.Time, qnames ...string) ([]*base.TaskMessage, error) {
	// For now, delegate to the original RDB implementation
	// TODO: Implement namespace-aware version
	return r.RDB.ListLeaseExpired(cutoff, qnames...)
}

// ExtendLease extends the lease for the given tasks with namespace support.
func (r *NamespaceRDB) ExtendLease(qname string, ids ...string) (time.Time, error) {
	// For now, delegate to the original RDB implementation
	// TODO: Implement namespace-aware version
	return r.RDB.ExtendLease(qname, ids...)
}

// WriteServerState writes server state data to redis with namespace support.
func (r *NamespaceRDB) WriteServerState(info *base.ServerInfo, workers []*base.WorkerInfo, ttl time.Duration) error {
	// For now, delegate to the original RDB implementation
	// TODO: Implement namespace-aware version
	return r.RDB.WriteServerState(info, workers, ttl)
}

// ClearServerState deletes server state data from redis with namespace support.
func (r *NamespaceRDB) ClearServerState(host string, pid int, serverID string) error {
	// For now, delegate to the original RDB implementation
	// TODO: Implement namespace-aware version
	return r.RDB.ClearServerState(host, pid, serverID)
}

// CancelationPubSub returns a pubsub for cancelation messages with namespace support.
func (r *NamespaceRDB) CancelationPubSub() (*redis.PubSub, error) {
	// For now, delegate to the original RDB implementation
	// TODO: Implement namespace-aware version
	return r.RDB.CancelationPubSub()
}

// PublishCancelation publish cancelation message with namespace support.
func (r *NamespaceRDB) PublishCancelation(id string) error {
	// For now, delegate to the original RDB implementation
	// TODO: Implement namespace-aware version
	return r.RDB.PublishCancelation(id)
}

// WriteResult writes the given result data for the specified task with namespace support.
func (r *NamespaceRDB) WriteResult(qname, id string, data []byte) (int, error) {
	// For now, delegate to the original RDB implementation
	// TODO: Implement namespace-aware version
	return r.RDB.WriteResult(qname, id, data)
}
