// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package scheduler

import (
	"go.uber.org/zap/zaptest"
	"testing"
	"yunli.com/jobpool/api/v2/domain"
)

func testJobBirdEyeViewInstance(t *testing.T) *JobBirdEyeView {
	lg := zaptest.NewLogger(t)
	tr, err := NewJobBirdEyeView(100, lg)
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	return tr
}

func Test_Enqueue_Dequeue(t *testing.T) {
	Parallel(t)
	pq := testJobBirdEyeViewInstance(t)
	if pq.Enabled() {
		t.Fatalf("should not be enabled")
	}
	pq.SetEnabled(true)
	if !pq.Enabled() {
		t.Fatalf("should be enabled")
	}
	namespace := "default"
	jobId := "1111-2222-3333-4444"
	eval := &domain.Evaluation{
		ID:        "12345678",
		Namespace: namespace,
		JobID:     jobId,
	}
	pq.Enqueue(eval)

	eval2 := &domain.Evaluation{
		ID:        "123456789",
		Namespace: namespace,
		JobID:     jobId,
	}
	pq.Enqueue(eval2)

	err := pq.DequeueJob(namespace, jobId)
	if err != nil {
		panic("dequeue eval failed")
	}
}
