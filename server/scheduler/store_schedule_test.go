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
	"fmt"
	"go.uber.org/zap"
	"golang.org/x/crypto/bcrypt"
	"testing"
	"yunli.com/jobpool/api/v2/domain"
	pb "yunli.com/jobpool/api/v2/etcdserverpb"
	"yunli.com/jobpool/server/v2/mock/mockscheduler"
	betesting "yunli.com/jobpool/server/v2/mvcc/backend/testing"
)

func setupScheduleStore(t *testing.T) (store *scheduleStore, teardownfunc func(t *testing.T)) {
	b, _ := betesting.NewDefaultTmpBackend(t)

	as := NewScheduleStore(zap.NewExample(), b, bcrypt.MinCost)

	namespace := "default"
	// adds a new namespace
	_, err := as.NameSpaceAdd(&pb.ScheduleNameSpaceAddRequest{Name: namespace})
	if err != nil {
		t.Fatal(err)
	}

	resp, err := as.NameSpaceDetail(&pb.ScheduleNameSpaceDetailRequest{
		Name: namespace,
	})
	if err != nil {
		t.Fatal(err)
	}
	if resp == nil || resp.Data == nil {
		t.Fatal(domain.NewErr1001Blank(fmt.Sprintf("ns: %s not exist", namespace)))
	}
	if resp.Data.Name != namespace {
		t.Fatal(domain.NewErr1002LimitValue("namespace", namespace))
	}
	tearDown := func(_ *testing.T) {
		b.Close()
		as.Close()
	}
	return as, tearDown
}

func TestPlanAdd(t *testing.T) {
	as, tearDown := setupScheduleStore(t)
	defer tearDown(t)

	plan, err := as.PlanAdd(mockscheduler.PlanAddMock()) // add an existing user
	if err != nil {
		t.Fatal(err)
	}
	if plan == nil || plan.Data == nil {
		t.Fatal(domain.NewErr1101None("plan", "plan"))
	}

}
