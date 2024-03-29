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

package time

import (
	"database/sql/driver"
	"time"
)

// FormatTime format time
type FormatTime string

// NewFormatTime net formatTime
func NewFormatTime(t time.Time) FormatTime {
	ft := new(FormatTime)
	ft.Scan(t)
	return *ft
}

// Scan scan time.
func (jt *FormatTime) Scan(src interface{}) (err error) {
	switch sc := src.(type) {
	case time.Time:
		*jt = FormatTime(sc.Format("2006-01-02 15:04:05"))
	case string:
		*jt = FormatTime(sc)
	}
	return
}

// Value get string value.
func (jt FormatTime) Value() (driver.Value, error) {
	return string(jt), nil
}

// TimeValue get time value.
func (jt FormatTime) TimeValue() time.Time {
	t, _ := time.ParseInLocation("2006-01-02 15:04:05", string(jt), time.Local)
	if t.Unix() <= 0 {
		t, _ = time.ParseInLocation("2006-01-02 15:04:05", "0000-00-00 00:00:00", time.Local)
	}
	return t
}

// UnmarshalJSON implement Unmarshaler
func (jt *FormatTime) UnmarshalJSON(data []byte) error {
	if data == nil || len(data) <= 1 {
		*jt = FormatTime("0000-00-00 00:00:00")
		return nil
	}
	str := string(data[1 : len(data)-1])
	st, err := time.Parse(time.RFC3339, str)
	if err == nil {
		*jt = FormatTime(st.Format("2006-01-02 15:04:05"))
	} else {
		*jt = FormatTime(str)
	}

	return nil
}
