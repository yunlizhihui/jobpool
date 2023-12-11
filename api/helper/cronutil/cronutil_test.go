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
	"regexp"
	"testing"
	"time"
)

func TestCron(t *testing.T) {
	spec := "0/30 * * * *"
	now := time.Now()
	time, err := Next(spec, now)
	if err != nil {
		fmt.Println("error ocur")
		fmt.Println(err)
		// t.Fatalf("fail next cron ", err)
	}
	fmt.Println(time)

}

func TestAfterNoonCron(t *testing.T) {
	spec := "01 00 * * *"
	now := time.Now()
	time, err := NextWithZone(spec, "", now)
	if err != nil {
		fmt.Println("error ocur")
		fmt.Println(err)
		// t.Fatalf("fail next cron ", err)
	}
	fmt.Println(time)

}

func TestRegex(t *testing.T) {
	var hasNamespaceFilter = regexp.MustCompile(`Namespace[\s]+==`)
	info := `Namespace   == "default"`
	if hasNamespaceFilter.Match([]byte(info)) {
		fmt.Println("ok")
	} else {
		fmt.Println("not")
	}
}
