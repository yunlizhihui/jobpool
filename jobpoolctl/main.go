// Copyright 2016 The etcd Authors
//
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

// etcdctl is a command line application that controls etcd.
package main

import (
	"os"

	"yunli.com/jobpool/jobpoolctl/v2/ctlv3"
)

const (
	apiEnv = "ETCDCTL_API"
)

/*
mainWithError is fully analogous to main, but instead of signaling errors
by os.Exit, it exposes the error explicitly, such that test-logic can intercept
control to e.g. dump coverage data (even for test-for-failure scenarios).
*/
func mainWithError() error {
	// unset apiEnv to avoid side-effect for future env and flag parsing.
	os.Unsetenv(apiEnv)
	return ctlv3.Start()

}

func main() {
	// unset apiEnv to avoid side-effect for future env and flag parsing.
	os.Unsetenv(apiEnv)
	ctlv3.MustStart()
	return

}
