/*
 * Copyright (c) 2009 The Go Authors. All rights reserved.
 * Copyright (c) 2024 Damian Peckett <damian@pecke.tt>.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are
 * met:
 *
 *    * Redistributions of source code must retain the above copyright
 * notice, this list of conditions and the following disclaimer.
 *    * Redistributions in binary form must reproduce the above
 * copyright notice, this list of conditions and the following disclaimer
 * in the documentation and/or other materials provided with the
 * distribution.
 *    * Neither the name of Google Inc. nor the names of its
 * contributors may be used to endorse or promote products derived from
 * this software without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
 * "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
 * LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
 * A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
 * OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
 * SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
 * LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
 * DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
 * THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
 * OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */

package par

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestQueue(t *testing.T) {
	t.Run("Idle", func(t *testing.T) {
		q := NewQueue(1)
		select {
		case <-q.Idle():
		default:
			t.Errorf("NewQueue(1) is not initially idle.")
		}

		started := make(chan struct{})
		unblock := make(chan struct{})
		q.Add(func() {
			close(started)
			<-unblock
		})

		<-started
		idle := q.Idle()
		select {
		case <-idle:
			t.Errorf("NewQueue(1) is marked idle while processing work.")
		default:
		}

		close(unblock)
		<-idle // Should be closed as soon as the Add callback returns.
	})

	t.Run("Backlog", func(t *testing.T) {
		const (
			maxActive = 2
			totalWork = 3 * maxActive
		)

		q := NewQueue(maxActive)

		var wg sync.WaitGroup
		wg.Add(totalWork)
		started := make([]chan struct{}, totalWork)
		unblock := make(chan struct{})
		for i := range started {
			started[i] = make(chan struct{})
			i := i
			q.Add(func() {
				close(started[i])
				<-unblock
				wg.Done()
			})
		}

		for i, c := range started {
			if i < maxActive {
				<-c // Work item i should be started immediately.
			} else {
				select {
				case <-c:
					t.Errorf("Work item %d started before previous items finished.", i)
				default:
				}
			}
		}

		close(unblock)
		for _, c := range started[maxActive:] {
			<-c
		}
		wg.Wait()
	})

	t.Run("MaxActive", func(t *testing.T) {
		const (
			maxActive = 5
			taskCount = 100
		)
		var (
			active int32
			max    int32
		)
		q := NewQueue(maxActive)

		var wg sync.WaitGroup
		wg.Add(taskCount)

		for i := 0; i < taskCount; i++ {
			q.Add(func() {
				atomic.AddInt32(&active, 1)
				current := atomic.LoadInt32(&active)
				if current > max {
					atomic.CompareAndSwapInt32(&max, max, current)
				}
				time.Sleep(10 * time.Millisecond) // simulate work
				atomic.AddInt32(&active, -1)
				wg.Done()
			})
		}

		wg.Wait()

		if max > maxActive {
			t.Errorf("Exceeded maxActive limit: max concurrent active tasks = %d, maxActive = %d", max, maxActive)
		}
	})

	t.Run("Clear", func(t *testing.T) {
		q := NewQueue(1)

		// Block the queue.
		q.Add(func() {
			time.Sleep(10 * time.Millisecond)
		})

		// Add a queued task that should not be executed just yet.
		var executed int32
		q.Add(func() {
			atomic.AddInt32(&executed, 1)
		})

		q.Clear()

		time.Sleep(20 * time.Millisecond)

		if atomic.LoadInt32(&executed) != 0 {
			t.Errorf("Task was executed after Clear was called")
		}

		// Ensure queue is still operational after Clear
		q.Add(func() {
			atomic.AddInt32(&executed, 1)
		})

		time.Sleep(10 * time.Millisecond)

		if atomic.LoadInt32(&executed) != 1 {
			t.Errorf("Queue did not execute task added after Clear")
		}
	})

}
