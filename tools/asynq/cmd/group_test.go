package cmd

import (
	"io"
	"os"
	"strings"
	"testing"
)

func TestGroupListRedisError(t *testing.T) {
	addr := os.Getenv("ASYNQ_TEST_REDIS_ADDR")
	if addr == "" {
		t.Skip("set ASYNQ_TEST_REDIS_ADDR to an isolated Redis instance")
	}

	rootCmd.SetArgs([]string{"group", "list", "--queue=probe", "--uri=" + addr, "--db=99", "--config=" + t.TempDir() + "/absent.yaml"})
	t.Cleanup(func() { rootCmd.SetArgs(nil) })
	err := rootCmd.Execute()
	if err == nil {
		t.Fatal("group list returned success for a Redis SELECT error")
	}
	if !strings.Contains(err.Error(), "DB index is out of range") {
		t.Fatalf("group list error = %v, want Redis SELECT failure", err)
	}
}

func TestGroupListEmpty(t *testing.T) {
	addr := os.Getenv("ASYNQ_TEST_REDIS_ADDR")
	if addr == "" {
		t.Skip("set ASYNQ_TEST_REDIS_ADDR to an isolated Redis instance")
	}

	rootCmd.SetArgs([]string{"group", "list", "--queue=probe", "--namespace=cli-empty-test", "--uri=" + addr, "--db=14", "--config=" + t.TempDir() + "/absent.yaml"})
	t.Cleanup(func() { rootCmd.SetArgs(nil) })
	stdout := os.Stdout
	reader, writer, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	os.Stdout = writer
	t.Cleanup(func() { os.Stdout = stdout; reader.Close(); writer.Close() })
	runErr := rootCmd.Execute()
	writer.Close()
	output, readErr := io.ReadAll(reader)
	if readErr != nil {
		t.Fatal(readErr)
	}
	if runErr != nil {
		t.Fatalf("group list returned error on an empty namespace: %v", runErr)
	}
	if got, want := string(output), "No groups found in queue \"probe\"\n"; got != want {
		t.Fatalf("group list output = %q, want %q", got, want)
	}
}
