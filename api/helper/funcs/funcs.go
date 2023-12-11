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

package funcs

import "time"

// StopFunc is used to stop a time.Timer created with NewSafeTimer
type StopFunc func()

func NewSafeTimer(duration time.Duration) (*time.Timer, StopFunc) {
	if duration <= 0 {
		duration = 1
	}

	t := time.NewTimer(duration)
	cancel := func() {
		t.Stop()
	}

	return t, cancel
}

func CopySliceByte(s []byte) []byte {
	l := len(s)
	if l == 0 {
		return nil
	}

	c := make([]byte, l)
	copy(c, s)
	return c
}
