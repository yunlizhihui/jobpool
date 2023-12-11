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

package domain

import (
	"fmt"
)

// NamespacedID is a tuple of an ID and a namespace
type NamespacedID struct {
	ID        string
	Namespace string
}

// NewNamespacedID returns a new namespaced ID given the ID and namespace
func NewNamespacedID(id, ns string) NamespacedID {
	return NamespacedID{
		ID:        id,
		Namespace: ns,
	}
}

func (n NamespacedID) String() string {
	return fmt.Sprintf("<ns: %q, id: %q>", n.Namespace, n.ID)
}
