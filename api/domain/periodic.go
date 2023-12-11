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
	"time"
	"yunli.com/jobpool/api/v2/helper/cronutil"
	xtime "yunli.com/jobpool/api/v2/helper/time"
)

const (
	// PeriodicSpecCron is used for a cron spec.
	PeriodicSpecCron = "cron"
)

// PeriodicConfig 调度相关的配置
type PeriodicConfig struct {
	Enabled bool `json:"enabled"`

	// Spec cron表达式
	Spec string `json:"spec"`

	// SpecType 类型，默认为cron
	SpecType string `json:"specType"`

	// ProhibitOverlap 是否允许并发执行
	ProhibitOverlap bool `json:"prohibitOverlap"`

	// TimeZone 时区 by default is "Asia/Shanghai"
	TimeZone string `json:"tz"`

	// location is the time zone to evaluate the launch time against
	location *time.Location

	// scheduler after this time
	StartTime xtime.FormatTime `json:"startTime"`

	// scheduler expire after this time
	EndTime xtime.FormatTime `json:"endTime"`
}

// Next 获取下一次执行的时间
func (p *PeriodicConfig) Next(fromTime time.Time) (time.Time, error) {
	return cronutil.NextWithZone(p.Spec, p.TimeZone, fromTime)
}

// GetLocation 获取时区信息.
func (p *PeriodicConfig) GetLocation() *time.Location {
	// Plans pre 0.5.5 will not have this
	if p.location != nil {
		return p.location
	}
	location, err := time.LoadLocation("Asia/Shanghai")
	if err != nil {
		return time.UTC
	}
	return location
}

func (p *PeriodicConfig) Canonicalize() {
	p.location = p.GetLocation()
}

func (p *PeriodicConfig) Merge(b *PeriodicConfig) *PeriodicConfig {
	var result *PeriodicConfig
	if p == nil {
		result = &PeriodicConfig{}
	} else {
		newP := new(PeriodicConfig)
		*newP = *p
		result = newP
	}
	if b != nil {
		if &b.Enabled != nil {
			result.Enabled = b.Enabled
		}
		if b.Spec != "" {
			result.Spec = b.Spec
		}
		if b.SpecType != "" {
			result.SpecType = b.SpecType
		}
		if &b.ProhibitOverlap != nil {
			result.ProhibitOverlap = b.ProhibitOverlap
		}
		if b.TimeZone != "" {
			result.TimeZone = b.TimeZone
		}
		if b.location != nil {
			result.location = b.location
		}
		if b.StartTime != "" {
			result.StartTime = b.StartTime
		}
		if b.EndTime != "" {
			result.EndTime = b.EndTime
		}
	}
	return result
}
