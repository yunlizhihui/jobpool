// Copyright 2021 The etcd Authors
//
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

package buckets

import (
	"bytes"

	"yunli.com/jobpool/server/v2/mvcc/backend"
)

var (
	keyBucketName   = []byte("key")
	metaBucketName  = []byte("meta")
	leaseBucketName = []byte("lease")
	alarmBucketName = []byte("alarm")

	clusterBucketName = []byte("cluster")

	membersBucketName        = []byte("members")
	membersRemovedBucketName = []byte("members_removed")

	authBucketName      = []byte("auth")
	authUsersBucketName = []byte("authUsers")
	authRolesBucketName = []byte("authRoles")

	scheduleBucketName            = []byte("schedule")
	schedulePlansBucketName       = []byte("schedulePlans")
	scheduleNameSpacesBucketName  = []byte("scheduleNameSpaces")
	scheduleJobsBucketName        = []byte("scheduleJobs")
	scheduleEvaluationsBucketName = []byte("scheduleEvaluations")
	scheduleAllocationsBucketName = []byte("scheduleAllocations")
	nodeBucketName                = []byte("nodes")
	scheduleLaunchesBucketName    = []byte("scheduleLaunches")

	testBucketName = []byte("test")
)

var (
	Key     = backend.Bucket(bucket{id: 1, name: keyBucketName, safeRangeBucket: true})
	Meta    = backend.Bucket(bucket{id: 2, name: metaBucketName, safeRangeBucket: false})
	Lease   = backend.Bucket(bucket{id: 3, name: leaseBucketName, safeRangeBucket: false})
	Alarm   = backend.Bucket(bucket{id: 4, name: alarmBucketName, safeRangeBucket: false})
	Cluster = backend.Bucket(bucket{id: 5, name: clusterBucketName, safeRangeBucket: false})

	Members        = backend.Bucket(bucket{id: 10, name: membersBucketName, safeRangeBucket: false})
	MembersRemoved = backend.Bucket(bucket{id: 11, name: membersRemovedBucketName, safeRangeBucket: false})

	Auth      = backend.Bucket(bucket{id: 20, name: authBucketName, safeRangeBucket: false})
	AuthUsers = backend.Bucket(bucket{id: 21, name: authUsersBucketName, safeRangeBucket: false})
	AuthRoles = backend.Bucket(bucket{id: 22, name: authRolesBucketName, safeRangeBucket: false})

	Schedule            = backend.Bucket(bucket{id: 30, name: scheduleBucketName, safeRangeBucket: false})
	SchedulePlans       = backend.Bucket(bucket{id: 31, name: schedulePlansBucketName, safeRangeBucket: false})
	ScheduleJobs        = backend.Bucket(bucket{id: 32, name: scheduleJobsBucketName, safeRangeBucket: false})
	ScheduleEvaluations = backend.Bucket(bucket{id: 33, name: scheduleEvaluationsBucketName, safeRangeBucket: false})
	ScheduleAllocations = backend.Bucket(bucket{id: 34, name: scheduleAllocationsBucketName, safeRangeBucket: false})
	Node                = backend.Bucket(bucket{id: 35, name: nodeBucketName, safeRangeBucket: false})
	ScheduleLaunches    = backend.Bucket(bucket{id: 36, name: scheduleLaunchesBucketName, safeRangeBucket: false})
	ScheduleNameSpaces  = backend.Bucket(bucket{id: 37, name: scheduleNameSpacesBucketName, safeRangeBucket: false})
	Test                = backend.Bucket(bucket{id: 100, name: testBucketName, safeRangeBucket: false})
)

type bucket struct {
	id              backend.BucketID
	name            []byte
	safeRangeBucket bool
}

func (b bucket) ID() backend.BucketID    { return b.id }
func (b bucket) Name() []byte            { return b.name }
func (b bucket) String() string          { return string(b.Name()) }
func (b bucket) IsSafeRangeBucket() bool { return b.safeRangeBucket }

var (
	MetaConsistentIndexKeyName = []byte("consistent_index")
	MetaTermKeyName            = []byte("term")
)

// DefaultIgnores defines buckets & keys to ignore in hash checking.
func DefaultIgnores(bucket, key []byte) bool {
	// consistent index & term might be changed due to v2 internal sync, which
	// is not controllable by the user.
	return bytes.Compare(bucket, Meta.Name()) == 0 &&
		(bytes.Compare(key, MetaTermKeyName) == 0 || bytes.Compare(key, MetaConsistentIndexKeyName) == 0)
}
