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

package runner

import (
	"go.uber.org/zap"
	"yunli.com/jobpool/agent/v2/service"
	"yunli.com/jobpool/api/v2/domain"
)

type Config struct {
	Logger       *zap.Logger
	Alloc        *domain.Allocation
	StateUpdater service.AllocStateHandler
}

type State struct {
	ClientStatus string
	JobStatus    string
	Info         string
}
