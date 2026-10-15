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
	"time"

	"k8s.io/apimachinery/pkg/util/wait"
)

const contextLockRetryInterval = 10 * time.Millisecond

// LockContext acquires a standard mutex through TryLock, honoring cancellation
// before and after acquisition. On success the caller owns the lock; on error
// no lock is held. unlock releases an acquisition if cancellation won the race.
func LockContext(ctx context.Context, tryLock func() bool, unlock func()) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	// Avoid a timer allocation on the uncontended path.
	if tryLock() {
		if err := ctx.Err(); err != nil {
			unlock()
			return err
		}
		return nil
	}
	return wait.PollUntilContextCancel(ctx, contextLockRetryInterval, true, func(context.Context) (bool, error) {
		if err := ctx.Err(); err != nil {
			return false, err
		}
		if !tryLock() {
			return false, nil
		}
		if err := ctx.Err(); err != nil {
			unlock()
			return false, err
		}
		return true, nil
	})
}
