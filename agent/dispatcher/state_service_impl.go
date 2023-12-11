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

package dispatcher

import "yunli.com/jobpool/api/v2/domain"

func (d *Dispatcher) GetAllocJobStatus(alloc *domain.Allocation) string {
	// TODO
	return "pending"
}

func (d *Dispatcher) AllocStateUpdated(alloc *domain.Allocation) {
	if alloc.Terminated() {
		// TODO do somting like gc
	}

	// Strip all the information that can be reconstructed at the server.  Only
	// send the fields that are updatable by the client.
	stripped := new(domain.Allocation)
	stripped.ID = alloc.ID
	stripped.JobId = alloc.JobId
	stripped.Namespace = alloc.Namespace
	stripped.NodeID = d.NodeID()
	stripped.ClientStatus = alloc.ClientStatus
	stripped.ClientDescription = alloc.ClientDescription

	select {
	case d.allocUpdates <- stripped:
	case <-d.shutdownCh:
	}
}
