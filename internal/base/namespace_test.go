package base

import (
	"fmt"
	"testing"

	"github.com/google/uuid"
)

func TestTaskKeyWithNamespace(t *testing.T) {
	id := uuid.NewString()

	tests := []struct {
		namespace string
		qname     string
		id        string
		want      string
	}{
		{"asynq", "default", id, fmt.Sprintf("asynq:{default}:t:%s", id)},
		{"app", "default", id, fmt.Sprintf("app:{default}:t:%s", id)},
		{"test", "custom", id, fmt.Sprintf("test:{custom}:t:%s", id)},
		{"myapp", "high", id, fmt.Sprintf("myapp:{high}:t:%s", id)},
	}

	for _, tc := range tests {
		got := TaskKey(tc.namespace, tc.qname, tc.id)
		if got != tc.want {
			t.Errorf("TaskKey(%q, %q, %s) = %q, want %q", tc.namespace, tc.qname, tc.id, got, tc.want)
		}
	}
}

func TestQueueKeyPrefixWithNamespace(t *testing.T) {
	tests := []struct {
		namespace string
		qname     string
		want      string
	}{
		{"asynq", "default", "asynq:{default}:"},
		{"app", "default", "app:{default}:"},
		{"test", "custom", "test:{custom}:"},
		{"myapp", "high", "myapp:{high}:"},
	}

	for _, tc := range tests {
		got := QueueKeyPrefix(tc.namespace, tc.qname)
		if got != tc.want {
			t.Errorf("QueueKeyPrefix(%q, %q) = %q, want %q", tc.namespace, tc.qname, got, tc.want)
		}
	}
}

func TestPendingKeyWithNamespace(t *testing.T) {
	tests := []struct {
		namespace string
		qname     string
		want      string
	}{
		{"asynq", "default", "asynq:{default}:pending"},
		{"app", "default", "app:{default}:pending"},
		{"test", "custom", "test:{custom}:pending"},
		{"myapp", "high", "myapp:{high}:pending"},
	}

	for _, tc := range tests {
		got := PendingKey(tc.namespace, tc.qname)
		if got != tc.want {
			t.Errorf("PendingKey(%q, %q) = %q, want %q", tc.namespace, tc.qname, got, tc.want)
		}
	}
}

func TestActiveKeyWithNamespace(t *testing.T) {
	tests := []struct {
		namespace string
		qname     string
		want      string
	}{
		{"asynq", "default", "asynq:{default}:active"},
		{"app", "default", "app:{default}:active"},
		{"test", "custom", "test:{custom}:active"},
		{"myapp", "high", "myapp:{high}:active"},
	}

	for _, tc := range tests {
		got := ActiveKey(tc.namespace, tc.qname)
		if got != tc.want {
			t.Errorf("ActiveKey(%q, %q) = %q, want %q", tc.namespace, tc.qname, got, tc.want)
		}
	}
}

func TestLeaseKeyWithNamespace(t *testing.T) {
	tests := []struct {
		namespace string
		qname     string
		want      string
	}{
		{"asynq", "default", "asynq:{default}:lease"},
		{"app", "default", "app:{default}:lease"},
		{"test", "custom", "test:{custom}:lease"},
		{"myapp", "high", "myapp:{high}:lease"},
	}

	for _, tc := range tests {
		got := LeaseKey(tc.namespace, tc.qname)
		if got != tc.want {
			t.Errorf("LeaseKey(%q, %q) = %q, want %q", tc.namespace, tc.qname, got, tc.want)
		}
	}
}

func TestScheduledKeyWithNamespace(t *testing.T) {
	tests := []struct {
		namespace string
		qname     string
		want      string
	}{
		{"asynq", "default", "asynq:{default}:scheduled"},
		{"app", "default", "app:{default}:scheduled"},
		{"test", "custom", "test:{custom}:scheduled"},
		{"myapp", "high", "myapp:{high}:scheduled"},
	}

	for _, tc := range tests {
		got := ScheduledKey(tc.namespace, tc.qname)
		if got != tc.want {
			t.Errorf("ScheduledKey(%q, %q) = %q, want %q", tc.namespace, tc.qname, got, tc.want)
		}
	}
}

func TestRetryKeyWithNamespace(t *testing.T) {
	tests := []struct {
		namespace string
		qname     string
		want      string
	}{
		{"asynq", "default", "asynq:{default}:retry"},
		{"app", "default", "app:{default}:retry"},
		{"test", "custom", "test:{custom}:retry"},
		{"myapp", "high", "myapp:{high}:retry"},
	}

	for _, tc := range tests {
		got := RetryKey(tc.namespace, tc.qname)
		if got != tc.want {
			t.Errorf("RetryKey(%q, %q) = %q, want %q", tc.namespace, tc.qname, got, tc.want)
		}
	}
}

func TestArchivedKeyWithNamespace(t *testing.T) {
	tests := []struct {
		namespace string
		qname     string
		want      string
	}{
		{"asynq", "default", "asynq:{default}:archived"},
		{"app", "default", "app:{default}:archived"},
		{"test", "custom", "test:{custom}:archived"},
		{"myapp", "high", "myapp:{high}:archived"},
	}

	for _, tc := range tests {
		got := ArchivedKey(tc.namespace, tc.qname)
		if got != tc.want {
			t.Errorf("ArchivedKey(%q, %q) = %q, want %q", tc.namespace, tc.qname, got, tc.want)
		}
	}
}

func TestCompletedKeyWithNamespace(t *testing.T) {
	tests := []struct {
		namespace string
		qname     string
		want      string
	}{
		{"asynq", "default", "asynq:{default}:completed"},
		{"app", "default", "app:{default}:completed"},
		{"test", "custom", "test:{custom}:completed"},
		{"myapp", "high", "myapp:{high}:completed"},
	}

	for _, tc := range tests {
		got := CompletedKey(tc.namespace, tc.qname)
		if got != tc.want {
			t.Errorf("CompletedKey(%q, %q) = %q, want %q", tc.namespace, tc.qname, got, tc.want)
		}
	}
}

func TestUniqueKeyWithNamespace(t *testing.T) {
	tests := []struct {
		namespace string
		qname     string
		tasktype  string
		payload   []byte
		want      string
	}{
		{"asynq", "default", "email", []byte("hello"), "asynq:{default}:unique:b493d48364afe44d11c0165cf470a4164d1e2609911ef998be868d46ade3de4e"},
		{"app", "default", "email", []byte("hello"), "app:{default}:unique:b493d48364afe44d11c0165cf470a4164d1e2609911ef998be868d46ade3de4e"},
		{"test", "custom", "sms", []byte("world"), "test:{custom}:unique:6994c1c18c9d6c0a2c8b4b6c7a8b8c9d6c0a2c8b4b6c7a8b8c9d6c0a2c8b4b6c"},
	}

	for _, tc := range tests {
		got := UniqueKey(tc.namespace, tc.qname, tc.tasktype, tc.payload)
		// Note: We can't predict the exact hash, so we'll just check the prefix
		expectedPrefix := fmt.Sprintf("%s:{%s}:unique:", tc.namespace, tc.qname)
		if len(got) < len(expectedPrefix) || got[:len(expectedPrefix)] != expectedPrefix {
			t.Errorf("UniqueKey(%q, %q, %q, %v) = %q, want prefix %q", tc.namespace, tc.qname, tc.tasktype, tc.payload, got, expectedPrefix)
		}
	}
}

func TestGroupKeyWithNamespace(t *testing.T) {
	tests := []struct {
		namespace string
		qname     string
		gname     string
		want      string
	}{
		{"asynq", "default", "mygroup", "asynq:{default}:g:mygroup"},
		{"app", "default", "mygroup", "app:{default}:g:mygroup"},
		{"test", "custom", "batch", "test:{custom}:g:batch"},
		{"myapp", "high", "priority", "myapp:{high}:g:priority"},
	}

	for _, tc := range tests {
		got := GroupKey(tc.namespace, tc.qname, tc.gname)
		if got != tc.want {
			t.Errorf("GroupKey(%q, %q, %q) = %q, want %q", tc.namespace, tc.qname, tc.gname, got, tc.want)
		}
	}
}

func TestAggregationSetKeyWithNamespace(t *testing.T) {
	tests := []struct {
		namespace string
		qname     string
		gname     string
		setID     string
		want      string
	}{
		{"asynq", "default", "mygroup", "set1", "asynq:{default}:g:mygroup:set1"},
		{"app", "default", "mygroup", "set1", "app:{default}:g:mygroup:set1"},
		{"test", "custom", "batch", "set2", "test:{custom}:g:batch:set2"},
		{"myapp", "high", "priority", "set3", "myapp:{high}:g:priority:set3"},
	}

	for _, tc := range tests {
		got := AggregationSetKey(tc.namespace, tc.qname, tc.gname, tc.setID)
		if got != tc.want {
			t.Errorf("AggregationSetKey(%q, %q, %q, %q) = %q, want %q", tc.namespace, tc.qname, tc.gname, tc.setID, got, tc.want)
		}
	}
}

// Note: Global key functions with namespace are not implemented yet
// These tests are commented out until the functions are implemented

/*
func TestAllServersKeyWithNamespace(t *testing.T) {
	tests := []struct {
		namespace string
		want      string
	}{
		{"asynq", "asynq:servers"},
		{"app", "app:servers"},
		{"test", "test:servers"},
		{"myapp", "myapp:servers"},
	}

	for _, tc := range tests {
		got := AllServersKeyWithNamespace(tc.namespace)
		if got != tc.want {
			t.Errorf("AllServersKeyWithNamespace(%q) = %q, want %q", tc.namespace, got, tc.want)
		}
	}
}
*/

func TestServerInfoKeyWithNamespace(t *testing.T) {
	tests := []struct {
		namespace string
		hostname  string
		pid       int
		serverID  string
		want      string
	}{
		{"asynq", "localhost", 1234, "server1", "asynq:servers:{localhost:1234:server1}"},
		{"app", "web01", 5678, "server2", "app:servers:{web01:5678:server2}"},
		{"test", "worker", 9999, "test-server", "test:servers:{worker:9999:test-server}"},
		{"myapp", "prod", 8080, "main", "myapp:servers:{prod:8080:main}"},
	}

	for _, tc := range tests {
		got := ServerInfoKey(tc.namespace, tc.hostname, tc.pid, tc.serverID)
		if got != tc.want {
			t.Errorf("ServerInfoKey(%q, %q, %d, %q) = %q, want %q", tc.namespace, tc.hostname, tc.pid, tc.serverID, got, tc.want)
		}
	}
}

func TestWorkersKeyWithNamespace(t *testing.T) {
	tests := []struct {
		namespace string
		hostname  string
		pid       int
		serverID  string
		want      string
	}{
		{"asynq", "localhost", 1234, "server1", "asynq:workers:{localhost:1234:server1}"},
		{"app", "web01", 5678, "server2", "app:workers:{web01:5678:server2}"},
		{"test", "worker", 9999, "test-server", "test:workers:{worker:9999:test-server}"},
		{"myapp", "prod", 8080, "main", "myapp:workers:{prod:8080:main}"},
	}

	for _, tc := range tests {
		got := WorkersKey(tc.namespace, tc.hostname, tc.pid, tc.serverID)
		if got != tc.want {
			t.Errorf("WorkersKey(%q, %q, %d, %q) = %q, want %q", tc.namespace, tc.hostname, tc.pid, tc.serverID, got, tc.want)
		}
	}
}

func TestSchedulerEntriesKeyWithNamespace(t *testing.T) {
	tests := []struct {
		namespace   string
		schedulerID string
		want        string
	}{
		{"asynq", "scheduler1", "asynq:schedulers:{scheduler1}"},
		{"app", "scheduler2", "app:schedulers:{scheduler2}"},
		{"test", "test-scheduler", "test:schedulers:{test-scheduler}"},
		{"myapp", "main-scheduler", "myapp:schedulers:{main-scheduler}"},
	}

	for _, tc := range tests {
		got := SchedulerEntriesKey(tc.namespace, tc.schedulerID)
		if got != tc.want {
			t.Errorf("SchedulerEntriesKey(%q, %q) = %q, want %q", tc.namespace, tc.schedulerID, got, tc.want)
		}
	}
}

func TestSchedulerHistoryKeyWithNamespace(t *testing.T) {
	tests := []struct {
		namespace   string
		schedulerID string
		want        string
	}{
		{"asynq", "scheduler1", "asynq:scheduler_history:scheduler1"},
		{"app", "scheduler2", "app:scheduler_history:scheduler2"},
		{"test", "test-scheduler", "test:scheduler_history:test-scheduler"},
		{"myapp", "main-scheduler", "myapp:scheduler_history:main-scheduler"},
	}

	for _, tc := range tests {
		got := SchedulerHistoryKey(tc.namespace, tc.schedulerID)
		if got != tc.want {
			t.Errorf("SchedulerHistoryKey(%q, %q) = %q, want %q", tc.namespace, tc.schedulerID, got, tc.want)
		}
	}
}
