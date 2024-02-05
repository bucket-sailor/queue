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
	"fmt"
	"sync"
)

// Queue manages a set of work items to be executed in parallel. The number of
// active work items is limited, and excess items are queued sequentially.
type Queue struct {
	maxActive int
	st        chan queueState
	mu        sync.Mutex
}

type queueState struct {
	active  int // number of goroutines processing work; always nonzero when len(backlog) > 0
	backlog []func()
	idle    chan struct{} // if non-nil, closed when active becomes 0
}

// NewQueue returns a Queue that executes up to maxActive items in parallel.
//
// maxActive must be positive.
func NewQueue(maxActive int) *Queue {
	if maxActive < 1 {
		panic(fmt.Sprintf("par.NewQueue called with nonpositive limit (%d)", maxActive))
	}

	q := &Queue{
		maxActive: maxActive,
		st:        make(chan queueState, 1),
	}
	q.st <- queueState{}
	return q
}

// Add adds f as a work item in the queue.
//
// Add returns immediately, but the queue will be marked as non-idle until after
// f (and any subsequently-added work) has completed.
func (q *Queue) Add(f func()) {
	q.mu.Lock()
	defer q.mu.Unlock()

	st := <-q.st
	if st.active == q.maxActive {
		st.backlog = append(st.backlog, f)
	} else {
		if st.active == 0 {
			// Mark q as non-idle.
			st.idle = nil
		}
		st.active++
		go q.worker(f)
	}
	q.st <- st
}

func (q *Queue) worker(f func()) {
	defer func() {
		q.mu.Lock()

		st := <-q.st
		if len(st.backlog) == 0 {
			if st.active--; st.active == 0 && st.idle != nil {
				close(st.idle)
			}
		} else {
			nextF, newBacklog := st.backlog[0], st.backlog[1:]
			st.backlog = newBacklog
			q.st <- st
			q.mu.Unlock()
			q.worker(nextF)
			return
		}
		q.st <- st
		q.mu.Unlock()
	}()

	f()
}

// Idle returns a channel that will be closed when q has no (active or enqueued)
// work outstanding.
func (q *Queue) Idle() <-chan struct{} {
	q.mu.Lock()
	defer q.mu.Unlock()

	st := <-q.st
	if st.idle == nil {
		st.idle = make(chan struct{})
		if st.active == 0 {
			close(st.idle)
		}
	}
	q.st <- st
	return st.idle
}

// Clear removes all work items from the queue.
func (q *Queue) Clear() {
	q.mu.Lock()
	defer q.mu.Unlock()

	st := <-q.st
	st.backlog = nil
	q.st <- st
}
