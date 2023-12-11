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

package cronutil

import (
	"fmt"
	"github.com/robfig/cron/v3"
	"time"
)

const defaultTimeZone = "Asia/Shanghai"

// Next returns the closest time instant matching the spec that is after the
// passed time. If no matching instance exists, the zero value of time.Time is
// returned. The `time.Location` of the returned value matches that of the
// passed time.
func Next(spec string, fromTime time.Time) (time.Time, error) {
	s, err := cron.ParseStandard(spec)
	if err != nil {
		return time.Time{}, fmt.Errorf("failed parsing cron expression: %q: %v", spec, err)
	}
	fetchTime := s.Next(fromTime)
	return fetchTime, nil
}

func NextWithZone(spec string, tz string, fromTime time.Time) (time.Time, error) {
	if tz == "" {
		tz = defaultTimeZone
	}
	tzSpec := fmt.Sprintf("TZ=%s %s", tz, spec)
	s, err := cron.ParseStandard(tzSpec)
	if err != nil {
		return time.Time{}, fmt.Errorf("failed parsing cron expression: %q: %v", spec, err)
	}
	fetchTime := s.Next(fromTime)
	return fetchTime, nil
}
