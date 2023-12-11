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
	"encoding/json"
	"fmt"
	"testing"
	"time"
	"yunli.com/jobpool/api/v2/constant"
	"yunli.com/jobpool/client/pkg/v2/testutil"
	"yunli.com/jobpool/server/v2/mock/mockscheduler"
)

func testEvalBroker(t *testing.T, timeout time.Duration) *EvalBroker {
	b, err := NewEvalBroker(5*time.Second, 1*time.Second, 20*time.Second, 3)
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	return b
}

func Parallel(t *testing.T) {
	t.Parallel()
}

var (
	defaultSched = []string{
		constant.PlanTypeService,
	}
)

func TestEvaluationEnqueueDequeue(t *testing.T) {
	Parallel(t)
	b := testEvalBroker(t, 0)
	b.SetEnabled(true)

	eval := mockscheduler.EvaluationMock()
	b.Enqueue(eval)

	out, token, err := b.Dequeue(defaultSched, 5*time.Second)
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if out != eval {
		t.Fatalf("bad : %#v", out)
	}
	fmt.Println(token)
}

func TestEvaluationAck_Nack(t *testing.T) {
	Parallel(t)
	b := testEvalBroker(t, 0)

	// Enqueue, but broker is disabled!
	eval := mockscheduler.EvaluationMock()
	b.Enqueue(eval)

	// Verify nothing was done
	stats := b.Stats()
	if stats.TotalReady != 0 {
		t.Fatalf("bad: %#v", stats)
	}
	// Enable the broker, and enqueue
	b.SetEnabled(true)
	b.Enqueue(eval)

	// Double enqueue is a no-op
	b.Enqueue(eval)

	// Verify enqueue is done
	stats = b.Stats()
	if stats.TotalReady != 1 {
		t.Fatalf("bad: %#v", stats)
	}
	if stats.ByScheduler[eval.Type].Ready != 1 {
		t.Fatalf("bad: %#v", stats)
	}

	// Dequeue should work
	out, token, err := b.Dequeue([]string{constant.PlanTypeService}, time.Second)
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if out != eval {
		t.Fatalf("bad : %#v", out)
	}

	tokenOut, ok := b.Outstanding(out.ID)
	if !ok {
		t.Fatalf("should be outstanding")
	}
	if tokenOut != token {
		t.Fatalf("Bad: %#v %#v", token, tokenOut)
	}

	// OutstandingReset should verify the token
	err = b.OutstandingReset("nope", "foo")
	if err.Error() != "evaluation is not outstanding" {
		t.Fatalf("err: %v", err)
	}
	err = b.OutstandingReset(out.ID, "foo")
	if err.Error() != "evaluation token does not match" {
		t.Fatalf("err: %v", err)
	}
	err = b.OutstandingReset(out.ID, tokenOut)
	if err != nil {
		t.Fatalf("err: %v", err)
	}

	// Check the stats
	stats = b.Stats()
	statJson, err := json.Marshal(stats)
	fmt.Println(fmt.Sprintf("the stats is : %s", statJson))

	if stats.TotalReady != 0 {
		t.Fatalf("bad: %#v", stats)
	}
	if stats.TotalUnacked != 1 {
		t.Fatalf("bad: %#v", stats)
	}
	if stats.ByScheduler[eval.Type].Ready != 0 {
		t.Fatalf("bad: %#v", stats)
	}
	if stats.ByScheduler[eval.Type].Unacked != 1 {
		t.Fatalf("bad: %#v", stats)
	}

	err = b.Nack(eval.ID, token)
	if err != nil {
		t.Fatalf("err in nack: %#v", err)
	}

	stats = b.Stats()
	statJson, _ = json.Marshal(stats)
	fmt.Println(fmt.Sprintf("the new stats is : %s", statJson))

	if _, ok := b.Outstanding(out.ID); ok {
		t.Fatalf("should not be outstanding")
	}

	stats = b.Stats()
	statJson, _ = json.Marshal(stats)
	fmt.Println(fmt.Sprintf("after out stats is : %s", statJson))

	testutil.WaitForResult(func() (bool, error) {
		stats = b.Stats()
		statJson, _ = json.Marshal(stats)
		fmt.Println(fmt.Sprintf("in wait for result : %s", statJson))
		if stats.TotalReady != 1 {
			return false, fmt.Errorf("bad: %#v", stats)
		}
		if stats.TotalUnacked != 0 {
			return false, fmt.Errorf("bad: %#v", stats)
		}
		if stats.TotalWaiting != 0 {
			return false, fmt.Errorf("bad: %#v", stats)
		}
		if stats.ByScheduler[eval.Type].Ready != 1 {
			return false, fmt.Errorf("bad: %#v", stats)
		}
		if stats.ByScheduler[eval.Type].Unacked != 0 {
			return false, fmt.Errorf("bad: %#v", stats)
		}

		return true, nil
	}, func(e error) {
		t.Fatal(e)
	})

	// Dequeue should work again
	out2, token2, err := b.Dequeue([]string{constant.PlanTypeService}, time.Second)
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if out2 != eval {
		t.Fatalf("bad : %#v", out2)
	}
	if token2 == token {
		t.Fatalf("should get a new token")
	}
	stats = b.Stats()
	statJson, _ = json.Marshal(stats)
	fmt.Println(fmt.Sprintf("after dequeue stats is : %s", statJson))

	// Ack finally
	err = b.Ack(eval.ID, token2)
	if err != nil {
		t.Fatalf("err: %v", err)
	}

	stats = b.Stats()
	statJson, _ = json.Marshal(stats)
	fmt.Println(fmt.Sprintf("after ack stats is : %s", statJson))
}

func TestEvaluationBroker_Dequeue_Priority(t *testing.T) {
	Parallel(t)
	b := testEvalBroker(t, 0)
	b.SetEnabled(true)

	eval1 := mockscheduler.EvaluationMock()
	eval1.Priority = 10
	b.Enqueue(eval1)

	eval2 := mockscheduler.EvaluationMock()
	eval2.Priority = 30
	b.Enqueue(eval2)

	eval3 := mockscheduler.EvaluationMock()
	eval3.Priority = 20
	b.Enqueue(eval3)

	out1, _, _ := b.Dequeue(defaultSched, time.Second)
	if out1 != eval2 {
		t.Fatalf("bad: %#v", out1)
	}

	out2, _, _ := b.Dequeue(defaultSched, time.Second)
	if out2 != eval3 {
		t.Fatalf("bad: %#v", out2)
	}

	out3, _, _ := b.Dequeue(defaultSched, time.Second)
	if out3 != eval1 {
		t.Fatalf("bad: %#v", out3)
	}
}
