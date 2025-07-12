package asynq

import (
	"context"
	"testing"
	"time"

	"github.com/yetiz-org/asynq/internal/base"
	h "github.com/yetiz-org/asynq/internal/testutil"
)

func TestNewClientWithNamespace(t *testing.T) {
	r := setup(t)
	defer r.Close()

	tests := []struct {
		desc      string
		namespace string
		want      string
	}{
		{
			desc:      "default namespace",
			namespace: "asynq",
			want:      "asynq",
		},
		{
			desc:      "custom namespace",
			namespace: "myapp",
			want:      "myapp",
		},
		{
			desc:      "test namespace",
			namespace: "test",
			want:      "test",
		},
	}

	for _, tc := range tests {
		t.Run(tc.desc, func(t *testing.T) {
			client := NewClientWithNamespace(getRedisConnOpt(t), tc.namespace)
			defer client.Close()

			if client.namespace != tc.want {
				t.Errorf("NewClientWithNamespace namespace = %q, want %q", client.namespace, tc.want)
			}
		})
	}
}

func TestNewClientFromRedisClientWithNamespace(t *testing.T) {
	r := setup(t)
	defer r.Close()

	tests := []struct {
		desc      string
		namespace string
		want      string
	}{
		{
			desc:      "default namespace",
			namespace: "asynq",
			want:      "asynq",
		},
		{
			desc:      "custom namespace",
			namespace: "myapp",
			want:      "myapp",
		},
		{
			desc:      "test namespace",
			namespace: "test",
			want:      "test",
		},
	}

	for _, tc := range tests {
		t.Run(tc.desc, func(t *testing.T) {
			client := NewClientFromRedisClientWithNamespace(setup(t), tc.namespace)
			defer client.Close()

			if client.namespace != tc.want {
				t.Errorf("NewClientFromRedisClientWithNamespace namespace = %q, want %q", client.namespace, tc.want)
			}
		})
	}
}

func TestClientEnqueueWithNamespace(t *testing.T) {
	r := setup(t)
	defer r.Close()

	task := NewTask("send_email", h.JSON(map[string]interface{}{"to": "customer@gmail.com", "from": "merchant@example.com"}))

	tests := []struct {
		desc      string
		namespace string
		qname     string
		task      *Task
		opts      []Option
	}{
		{
			desc:      "enqueue with default namespace",
			namespace: "asynq",
			qname:     "default",
			task:      task,
			opts:      []Option{},
		},
		{
			desc:      "enqueue with custom namespace",
			namespace: "myapp",
			qname:     "default",
			task:      task,
			opts:      []Option{},
		},
		{
			desc:      "enqueue with test namespace and custom queue",
			namespace: "test",
			qname:     "high",
			task:      task,
			opts:      []Option{Queue("high")},
		},
	}

	for _, tc := range tests {
		t.Run(tc.desc, func(t *testing.T) {
			// Clean up before test
			h.FlushDB(t, r)

			client := NewClientWithNamespace(getRedisConnOpt(t), tc.namespace)
			defer client.Close()

			info, err := client.Enqueue(task, tc.opts...)
			if err != nil {
				t.Errorf("Client.Enqueue returned error: %v", err)
				return
			}

			// Verify task was enqueued with correct namespace
			expectedKey := base.PendingKey(tc.namespace, tc.qname)
			pendingTasks := r.LRange(context.Background(), expectedKey, 0, -1).Val()
			if len(pendingTasks) != 1 {
				t.Errorf("Expected 1 task in pending queue %q, got %d", expectedKey, len(pendingTasks))
			}

			// Verify task info has correct queue
			if info.Queue != tc.qname {
				t.Errorf("TaskInfo.Queue = %q, want %q", info.Queue, tc.qname)
			}
		})
	}
}

func TestClientEnqueueWithNamespaceIsolation(t *testing.T) {
	r := setup(t)
	defer r.Close()
	h.FlushDB(t, r)

	task := NewTask("send_email", h.JSON(map[string]interface{}{"to": "test@example.com"}))

	// Create clients with different namespaces
	client1 := NewClientWithNamespace(getRedisConnOpt(t), "app1")
	defer client1.Close()

	client2 := NewClientWithNamespace(getRedisConnOpt(t), "app2")
	defer client2.Close()

	// Enqueue tasks with different namespaces
	_, err := client1.Enqueue(task)
	if err != nil {
		t.Fatalf("client1.Enqueue failed: %v", err)
	}

	_, err = client2.Enqueue(task)
	if err != nil {
		t.Fatalf("client2.Enqueue failed: %v", err)
	}

	// Verify tasks are isolated by namespace
	app1Key := base.PendingKey("app1", "default")
	app2Key := base.PendingKey("app2", "default")

	app1Tasks := r.LRange(context.Background(), app1Key, 0, -1).Val()
	app2Tasks := r.LRange(context.Background(), app2Key, 0, -1).Val()

	if len(app1Tasks) != 1 {
		t.Errorf("Expected 1 task in app1 namespace, got %d", len(app1Tasks))
	}

	if len(app2Tasks) != 1 {
		t.Errorf("Expected 1 task in app2 namespace, got %d", len(app2Tasks))
	}

	// Verify tasks don't interfere with each other
	defaultKey := base.PendingKey("asynq", "default") // Original key without namespace
	defaultTasks := r.LRange(context.Background(), defaultKey, 0, -1).Val()
	if len(defaultTasks) != 0 {
		t.Errorf("Expected 0 tasks in default key (no namespace), got %d", len(defaultTasks))
	}
}

func TestServerWithNamespace(t *testing.T) {
	r := setup(t)
	defer r.Close()

	tests := []struct {
		desc      string
		namespace string
		want      string
	}{
		{
			desc:      "server with default namespace",
			namespace: "asynq",
			want:      "asynq",
		},
		{
			desc:      "server with custom namespace",
			namespace: "myapp",
			want:      "myapp",
		},
		{
			desc:      "server with test namespace",
			namespace: "test",
			want:      "test",
		},
	}

	for _, tc := range tests {
		t.Run(tc.desc, func(t *testing.T) {
			cfg := Config{
				Namespace:   tc.namespace,
				Concurrency: 1,
			}
			server := NewServer(getRedisConnOpt(t), cfg)
			defer server.Shutdown()

			if server.namespace != tc.want {
				t.Errorf("Server namespace = %q, want %q", server.namespace, tc.want)
			}
		})
	}
}

func TestServerWithNamespaceBackwardCompatibility(t *testing.T) {
	r := setup(t)
	defer r.Close()

	// Test that server without namespace in config defaults to "asynq"
	cfg := Config{
		Concurrency: 1,
		// Namespace not specified
	}
	server := NewServer(getRedisConnOpt(t), cfg)
	defer server.Shutdown()

	if server.namespace != "asynq" {
		t.Errorf("Server namespace = %q, want %q (default)", server.namespace, "asynq")
	}
}

func TestClientServerNamespaceIntegration(t *testing.T) {
	r := setup(t)
	defer r.Close()
	h.FlushDB(t, r)

	namespace := "integration_test"
	task := NewTask("test_task", h.JSON(map[string]interface{}{"data": "test"}))

	// Create client with namespace
	client := NewClientWithNamespace(getRedisConnOpt(t), namespace)
	defer client.Close()

	// Enqueue task
	_, err := client.Enqueue(task)
	if err != nil {
		t.Fatalf("Failed to enqueue task: %v", err)
	}

	// Verify task is in correct namespace key
	expectedKey := base.PendingKey(namespace, "default")
	pendingTasks := r.LRange(context.Background(), expectedKey, 0, -1).Val()
	if len(pendingTasks) != 1 {
		t.Errorf("Expected 1 task in namespace key %q, got %d", expectedKey, len(pendingTasks))
	}

	// Verify task is NOT in default namespace key
	defaultKey := base.PendingKey("", "default")
	defaultTasks := r.LRange(context.Background(), defaultKey, 0, -1).Val()
	if len(defaultTasks) != 0 {
		t.Errorf("Expected 0 tasks in default key %q, got %d", defaultKey, len(defaultTasks))
	}
}

func TestMultipleNamespaceClients(t *testing.T) {
	r := setup(t)
	defer r.Close()
	h.FlushDB(t, r)

	task1 := NewTask("task1", h.JSON(map[string]interface{}{"app": "app1"}))
	task2 := NewTask("task2", h.JSON(map[string]interface{}{"app": "app2"}))
	task3 := NewTask("task3", h.JSON(map[string]interface{}{"app": "app3"}))

	// Create clients with different namespaces
	clients := map[string]*Client{
		"app1": NewClientWithNamespace(getRedisConnOpt(t), "app1"),
		"app2": NewClientWithNamespace(getRedisConnOpt(t), "app2"),
		"app3": NewClientWithNamespace(getRedisConnOpt(t), "app3"),
	}

	// Clean up clients
	defer func() {
		for _, client := range clients {
			client.Close()
		}
	}()

	// Enqueue tasks
	_, err := clients["app1"].Enqueue(task1)
	if err != nil {
		t.Fatalf("Failed to enqueue task1: %v", err)
	}

	_, err = clients["app2"].Enqueue(task2)
	if err != nil {
		t.Fatalf("Failed to enqueue task2: %v", err)
	}

	_, err = clients["app3"].Enqueue(task3)
	if err != nil {
		t.Fatalf("Failed to enqueue task3: %v", err)
	}

	// Verify each namespace has exactly one task
	for namespace := range clients {
		key := base.PendingKey(namespace, "default")
		tasks := r.LRange(context.Background(), key, 0, -1).Val()
		if len(tasks) != 1 {
			t.Errorf("Expected 1 task in namespace %q, got %d", namespace, len(tasks))
		}
	}

	// Verify no tasks in default namespace
	defaultKey := base.PendingKey("", "default")
	defaultTasks := r.LRange(context.Background(), defaultKey, 0, -1).Val()
	if len(defaultTasks) != 0 {
		t.Errorf("Expected 0 tasks in default namespace, got %d", len(defaultTasks))
	}
}

func TestClientEnqueueScheduledWithNamespace(t *testing.T) {
	r := setup(t)
	defer r.Close()
	h.FlushDB(t, r)

	namespace := "scheduled_test"
	task := NewTask("scheduled_task", h.JSON(map[string]interface{}{"data": "scheduled"}))
	processAt := time.Now().Add(time.Hour)

	client := NewClientWithNamespace(getRedisConnOpt(t), namespace)
	defer client.Close()

	// Enqueue scheduled task
	info, err := client.Enqueue(task, ProcessAt(processAt))
	if err != nil {
		t.Fatalf("Failed to enqueue scheduled task: %v", err)
	}

	if info.State != TaskStateScheduled {
		t.Errorf("Expected task state to be %v, got %v", TaskStateScheduled, info.State)
	}

	// Verify task is in correct namespace scheduled key
	expectedKey := base.ScheduledKey(namespace, "default")
	scheduledTasks := r.ZRange(context.Background(), expectedKey, 0, -1).Val()
	if len(scheduledTasks) != 1 {
		t.Errorf("Expected 1 task in namespace scheduled key %q, got %d", expectedKey, len(scheduledTasks))
	}

	// Verify task is NOT in default namespace scheduled key
	defaultKey := base.ScheduledKey("", "default")
	defaultTasks := r.ZRange(context.Background(), defaultKey, 0, -1).Val()
	if len(defaultTasks) != 0 {
		t.Errorf("Expected 0 tasks in default scheduled key %q, got %d", defaultKey, len(defaultTasks))
	}
}

func TestClientEnqueueUniqueWithNamespace(t *testing.T) {
	r := setup(t)
	defer r.Close()
	h.FlushDB(t, r)

	namespace := "unique_test"
	task := NewTask("unique_task", h.JSON(map[string]interface{}{"data": "unique"}))

	client := NewClientWithNamespace(getRedisConnOpt(t), namespace)
	defer client.Close()

	// Enqueue unique task
	info1, err := client.Enqueue(task, Unique(time.Minute))
	if err != nil {
		t.Fatalf("Failed to enqueue first unique task: %v", err)
	}

	// Try to enqueue the same unique task again
	info2, err := client.Enqueue(task, Unique(time.Minute))
	if err == nil {
		t.Errorf("Expected error when enqueuing duplicate unique task, got nil")
	}

	if info2 != nil {
		t.Errorf("Expected nil TaskInfo for duplicate unique task, got %+v", info2)
	}

	// Verify unique key is created with correct namespace
	expectedUniqueKey := base.UniqueKey(namespace, "default", task.Type(), task.Payload())
	exists := r.Exists(context.Background(), expectedUniqueKey).Val()
	if exists != 1 {
		t.Errorf("Expected unique key %q to exist", expectedUniqueKey)
	}

	// Verify task is in correct namespace pending key
	expectedPendingKey := base.PendingKey(namespace, "default")
	pendingTasks := r.LRange(context.Background(), expectedPendingKey, 0, -1).Val()
	if len(pendingTasks) != 1 {
		t.Errorf("Expected 1 task in namespace pending key %q, got %d", expectedPendingKey, len(pendingTasks))
	}

	// Verify first task info
	if info1.State != TaskStatePending {
		t.Errorf("Expected first task state to be %v, got %v", TaskStatePending, info1.State)
	}
}
