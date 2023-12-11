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

package helper

import (
	"math/rand"
	"time"
)

func Uint64Max(a, b uint64) uint64 {
	if a > b {
		return a
	}
	return b
}

func MaxInt(a, b int) int {
	if a > b {
		return a
	}
	return b
}

// IntToPtr returns the pointer to an int
func IntToPtr(i int) *int {
	return &i
}

const (
	// minRate is the minimum rate at which we allow an action to be performed
	// across the whole cluster. The value is once a day: 1 / (1 * time.Day)
	minRate = 1.0 / 86400
)

// RandomStagger returns an interval between 0 and the duration
func RandomStagger(intv time.Duration) time.Duration {
	if intv == 0 {
		return 0
	}
	return time.Duration(uint64(rand.Int63()) % uint64(intv))
}

// RateScaledInterval is used to choose an interval to perform an action in
// order to target an aggregate number of actions per second across the whole
// cluster.
func RateScaledInterval(rate float64, min time.Duration, n int) time.Duration {
	if rate <= minRate {
		return min
	}
	interval := time.Duration(float64(time.Second) * float64(n) / rate)
	if interval < min {
		return min
	}

	return interval
}
