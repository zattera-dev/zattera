//go:build e2e

// Package e2e holds the full single-node smoke test (`make test-e2e`,
// task T-54): build the binary, boot `zattera server --dev`, deploy the
// go-hello fixture through the whole pipeline, assert the URL serves, test
// red/green + rollback, tear down.
package e2e
