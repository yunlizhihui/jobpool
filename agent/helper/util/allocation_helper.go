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

package util

import (
	"yunli.com/jobpool/agent/v2/structs"
	"yunli.com/jobpool/api/v2/domain"
)

type diffResult struct {
	Added   []*domain.Allocation
	Removed []string
	Updated []*domain.Allocation
	Ignore  []string
}

// DiffAllocs 更新后计算差别的allocation
func DiffAllocs(existing map[string]uint64, allocs *structs.AllocUpdates) *diffResult {
	// 扫描现有allocation
	result := &diffResult{}
	for existID, existIndex := range existing {
		// Check if the alloc was updated or filtered because an update wasn't
		// needed.
		alloc, pulled := allocs.Pulled[existID]
		_, filtered := allocs.Filtered[existID]

		// If not updated or filtered, removed
		if !pulled && !filtered {
			result.Removed = append(result.Removed, existID)
			continue
		}

		// Check for an update
		if pulled && alloc.AllocModifyIndex > existIndex {
			result.Updated = append(result.Updated, alloc)
			continue
		}

		// Ignore this
		result.Ignore = append(result.Ignore, existID)
	}

	// Scan the updated allocations for any that are new
	for id, pulled := range allocs.Pulled {
		if _, ok := existing[id]; !ok {
			result.Added = append(result.Added, pulled)
		}
	}
	return result
}
