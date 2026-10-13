package cmd

import (
	"context"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/yetiz-org/asynq"
)

func TestDashNamespace(t *testing.T) {
	addr := os.Getenv("ASYNQ_TEST_REDIS_ADDR")
	if addr == "" {
		t.Skip("set ASYNQ_TEST_REDIS_ADDR to an isolated Redis instance")
	}
	if _, err := exec.LookPath("script"); err != nil {
		t.Skip("script is required to run the dashboard with a terminal")
	}
	if runtime.GOOS != "darwin" && runtime.GOOS != "linux" {
		t.Skip("unsupported script command syntax")
	}

	client := redis.NewClient(&redis.Options{Addr: addr, DB: 14})
	t.Cleanup(func() { client.Close() })
	namespace := fmt.Sprintf("cli-dash-%d", time.Now().UnixNano())
	defaultQueue, selectedQueue, overrideQueue := namespace+"-default", namespace+"-selected", namespace+"-override"
	ctx := context.Background()
	for key, queue := range map[string]string{"asynq:queues": defaultQueue, namespace + ":queues": selectedQueue, namespace + "-override:queues": overrideQueue} {
		if err := client.SAdd(ctx, key, queue).Err(); err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() {
			client.SRem(ctx, key, queue)
			if key != "asynq:queues" {
				client.Del(ctx, key)
			}
		})
	}

	sharedQueue := "0-" + namespace
	selectedTaskID, defaultTaskID := namespace+"-selected-task", namespace+"-default-task"
	for _, fixture := range []struct{ namespace, taskID string }{
		{namespace: namespace, taskID: selectedTaskID},
		{namespace: "asynq", taskID: defaultTaskID},
	} {
		asynqClient := asynq.NewClientWithNamespace(asynq.RedisClientOpt{Addr: addr, DB: 14}, fixture.namespace)
		info, err := asynqClient.Enqueue(asynq.NewTask("cli:namespace-probe", nil), asynq.Queue(sharedQueue), asynq.TaskID(fixture.taskID))
		if err != nil {
			asynqClient.Close()
			t.Fatal(err)
		}
		if info.ID != fixture.taskID {
			t.Fatalf("enqueued task ID = %q, want %q", info.ID, fixture.taskID)
		}
		inspector := asynq.NewInspectorWithNamespace(asynq.RedisClientOpt{Addr: addr, DB: 14}, fixture.namespace)
		t.Cleanup(func() {
			if err := inspector.DeleteQueue(sharedQueue, true); err != nil {
				t.Errorf("remove %q queue in namespace %q: %v", sharedQueue, fixture.namespace, err)
			}
			inspector.Close()
			asynqClient.Close()
		})
	}

	for _, tc := range []struct {
		name, flag, config, want, taskWant, taskExclude string
		exclude                                         []string
	}{
		{name: "selected", flag: namespace, want: selectedQueue, exclude: []string{defaultQueue, overrideQueue}},
		{name: "config_only", flag: "default", config: namespace, want: selectedQueue, exclude: []string{defaultQueue, overrideQueue}},
		{name: "flag_overrides_config", flag: namespace + "-override", config: namespace, want: overrideQueue, exclude: []string{defaultQueue, selectedQueue}},
		{name: "default", flag: "default", want: defaultQueue, exclude: []string{selectedQueue, overrideQueue}},
		{name: "same_queue_tasks", flag: namespace, want: sharedQueue, taskWant: selectedTaskID, taskExclude: defaultTaskID},
	} {
		t.Run(tc.name, func(t *testing.T) {
			transcript := filepath.Join(t.TempDir(), "dashboard.txt")
			testBinary := os.Args[0]
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			var cmd *exec.Cmd
			if runtime.GOOS == "darwin" {
				cmd = exec.CommandContext(ctx, "script", "-qF", transcript, testBinary, "-test.run=^TestDashScreenHelper$")
			} else {
				cmd = exec.CommandContext(ctx, "script", "-q", "-f", "-c", testBinary+" -test.run=^TestDashScreenHelper$", transcript)
			}
			config := transcript + ".missing"
			if tc.config != "" {
				config = transcript + ".yaml"
				if err := os.WriteFile(config, []byte("namespace: "+tc.config+"\n"), 0600); err != nil {
					t.Fatal(err)
				}
			}
			cmd.Env = append(os.Environ(), "TERM=xterm-256color", "ASYNQ_DASH_TEST_NAMESPACE="+tc.flag, "ASYNQ_DASH_TEST_CONFIG="+config)
			input, err := cmd.StdinPipe()
			if err != nil {
				t.Fatal(err)
			}
			cmd.Stdout = io.Discard
			cmd.Stderr = io.Discard
			if err := cmd.Start(); err != nil {
				t.Fatal(err)
			}
			ticker := time.NewTicker(10 * time.Millisecond)
			defer ticker.Stop()
			var output []byte
		readOutput:
			for ctx.Err() == nil {
				<-ticker.C
				output, err = os.ReadFile(transcript)
				if err != nil && !os.IsNotExist(err) {
					break
				}
				if strings.Contains(string(output), tc.want) {
					break
				}
				for _, exclude := range tc.exclude {
					if strings.Contains(string(output), exclude) {
						break readOutput
					}
				}
			}
			if tc.taskWant != "" && strings.Contains(string(output), tc.want) {
				if _, err := io.WriteString(input, "j\r"); err != nil {
					t.Fatal(err)
				}
				for !strings.Contains(string(output), "=== Queue Summary ===") && ctx.Err() == nil {
					<-ticker.C
					output, err = os.ReadFile(transcript)
					if err != nil {
						break
					}
				}
				if _, err := io.WriteString(input, "l"); err != nil {
					t.Fatal(err)
				}
				for !strings.Contains(string(output), tc.taskWant) && !strings.Contains(string(output), tc.taskExclude) && ctx.Err() == nil {
					<-ticker.C
					output, err = os.ReadFile(transcript)
					if err != nil {
						break
					}
				}
			}
			_, writeErr := io.WriteString(input, "\x03")
			input.Close()
			waitErr := cmd.Wait()
			if err != nil && !os.IsNotExist(err) {
				t.Fatal(err)
			}
			if writeErr != nil || waitErr != nil {
				t.Fatalf("dashboard terminal did not exit cleanly: input=%v process=%v", writeErr, waitErr)
			}
			if !strings.Contains(string(output), tc.want) {
				t.Fatalf("dashboard did not render queue %q", tc.want)
			}
			for _, exclude := range tc.exclude {
				if strings.Contains(string(output), exclude) {
					t.Fatalf("dashboard rendered wrong namespace: queue %q appeared with %q", exclude, tc.want)
				}
			}
			if tc.taskWant != "" && (!strings.Contains(string(output), tc.taskWant) || strings.Contains(string(output), tc.taskExclude)) {
				t.Fatalf("dashboard task view should show %q but not %q", tc.taskWant, tc.taskExclude)
			}
		})
	}
}

func TestDashScreenHelper(t *testing.T) {
	ns := os.Getenv("ASYNQ_DASH_TEST_NAMESPACE")
	if ns == "" {
		return
	}
	flags := []string{"dash", "--uri=" + os.Getenv("ASYNQ_TEST_REDIS_ADDR"), "--db=14", "--refresh=1s", "--config=" + os.Getenv("ASYNQ_DASH_TEST_CONFIG")}
	if ns != "default" {
		flags = append(flags, "--namespace="+ns)
	}
	rootCmd.SetArgs(flags)
	if err := rootCmd.Execute(); err != nil {
		t.Fatal(err)
	}
}
