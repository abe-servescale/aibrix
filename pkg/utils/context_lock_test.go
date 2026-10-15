/*
Copyright 2026 The Aibrix Team.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package utils

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestContextLockCancelsHeldAndFreeLocks(t *testing.T) {
	var lock sync.RWMutex
	lock.Lock()
	ctx, cancel := context.WithCancel(context.Background())
	result := make(chan error, 1)
	go func() { result <- LockContext(ctx, lock.TryLock, lock.Unlock) }()
	cancel()
	select {
	case err := <-result:
		require.ErrorIs(t, err, context.Canceled)
	case <-time.After(time.Second):
		t.Fatal("lock wait ignored cancellation")
	}
	lock.Unlock()
	require.ErrorIs(t, LockContext(ctx, lock.TryLock, lock.Unlock), context.Canceled)
	require.True(t, lock.TryLock(), "a canceled immediate attempt must not leave a lock acquired")
	lock.Unlock()
}
