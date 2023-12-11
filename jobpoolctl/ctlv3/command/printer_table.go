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

package command

import (
	"os"
	pb "yunli.com/jobpool/api/v2/etcdserverpb"

	v3 "yunli.com/jobpool/client/v2"

	"github.com/olekukonko/tablewriter"
)

type tablePrinter struct{ printer }

func (tp *tablePrinter) MemberList(r v3.MemberListResponse) {
	hdr, rows := makeMemberListTable(r)
	table := tablewriter.NewWriter(os.Stdout)
	table.SetHeader(hdr)
	for _, row := range rows {
		table.Append(row)
	}
	table.SetAlignment(tablewriter.ALIGN_RIGHT)
	table.Render()
}
func (tp *tablePrinter) EndpointHealth(r []epHealth) {
	hdr, rows := makeEndpointHealthTable(r)
	table := tablewriter.NewWriter(os.Stdout)
	table.SetHeader(hdr)
	for _, row := range rows {
		table.Append(row)
	}
	table.SetAlignment(tablewriter.ALIGN_RIGHT)
	table.Render()
}
func (tp *tablePrinter) EndpointStatus(r []epStatus) {
	hdr, rows := makeEndpointStatusTable(r)
	table := tablewriter.NewWriter(os.Stdout)
	table.SetHeader(hdr)
	for _, row := range rows {
		table.Append(row)
	}
	table.SetAlignment(tablewriter.ALIGN_RIGHT)
	table.Render()
}
func (tp *tablePrinter) EndpointHashKV(r []epHashKV) {
	hdr, rows := makeEndpointHashKVTable(r)
	table := tablewriter.NewWriter(os.Stdout)
	table.SetHeader(hdr)
	for _, row := range rows {
		table.Append(row)
	}
	table.SetAlignment(tablewriter.ALIGN_RIGHT)
	table.Render()
}

func (tp *tablePrinter) PlanList(r pb.SchedulePlanListResponse) {
	hdr, rows := makePlanListTable(r)
	table := tablewriter.NewWriter(os.Stdout)
	table.SetHeader(hdr)
	for _, row := range rows {
		table.Append(row)
	}
	table.SetAlignment(tablewriter.ALIGN_RIGHT)
	table.Render()
}

func (tp *tablePrinter) PlanDetail(r pb.SchedulePlanDetailResponse) {
	hdr, rows := makePlanDetailTable(r)
	table := tablewriter.NewWriter(os.Stdout)
	table.SetHeader(hdr)
	for _, row := range rows {
		table.Append(row)
	}
	table.SetAlignment(tablewriter.ALIGN_RIGHT)
	table.Render()
}

func (tp *tablePrinter) JobList(r pb.ScheduleJobListResponse) {
	hdr, rows := makeJobListTable(r)
	table := tablewriter.NewWriter(os.Stdout)
	table.SetHeader(hdr)
	for _, row := range rows {
		table.Append(row)
	}
	table.SetAlignment(tablewriter.ALIGN_RIGHT)
	table.Render()
}

func (tp *tablePrinter) NamespaceList(r pb.ScheduleNameSpaceListResponse) {
	hdr, rows := makeNamespaceListTable(r)
	table := tablewriter.NewWriter(os.Stdout)
	table.SetHeader(hdr)
	for _, row := range rows {
		table.Append(row)
	}
	table.SetAlignment(tablewriter.ALIGN_RIGHT)
	table.Render()
}

func (tp *tablePrinter) EvalList(r pb.ScheduleEvalListResponse) {
	hdr, rows := makeEvalListTable(r)
	table := tablewriter.NewWriter(os.Stdout)
	table.SetHeader(hdr)
	for _, row := range rows {
		table.Append(row)
	}
	table.SetAlignment(tablewriter.ALIGN_RIGHT)
	table.Render()
}
func (tp *tablePrinter) AllocationList(r pb.ScheduleAllocationListResponse) {
	hdr, rows := makeAllocationListTable(r)
	table := tablewriter.NewWriter(os.Stdout)
	table.SetHeader(hdr)
	for _, row := range rows {
		table.Append(row)
	}
	table.SetAlignment(tablewriter.ALIGN_RIGHT)
	table.Render()
}
