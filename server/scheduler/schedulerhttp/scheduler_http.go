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

package schedulerhttp

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io/ioutil"
	"net/http"
	"time"
	"yunli.com/jobpool/api/v2/domain"
	pb "yunli.com/jobpool/api/v2/etcdserverpb"
	"yunli.com/jobpool/pkg/v2/httputil"
	"yunli.com/jobpool/server/v2/scheduler/queueservice"
)

var (
	EvalQueuePrefix          = "/schedule/evals/queue"
	EvalDequeuePrefix        = "/schedule/evals/dequeue"
	EvalAckPrefix            = "/schedule/evals/ack"
	EvalNackPrefix           = "/schedule/evals/nack"
	PlanAllocationAddPrefix  = "/schedule/plans/allocations/add"
	QueueDetailPrefix        = "/schedule/queues/detail"
	QueueJobViewDetailPrefix = "/schedule/queues/job/view/detail"
	QueueForbiddenPrefix     = "/schedule/queues/forbidden"
	QueueStatusPrefix        = "/schedule/queues/status"
	applyTimeout             = 5 * time.Second
	ErrLeaseHTTPTimeout      = errors.New("waiting for node to catch up its applied index has timed out")
	ErrScheduleNotFound      = errors.New("scheduler http not found")
)

// NewHandler returns an http Handler for schedule logic
func NewHandler(q *queueservice.QueueOperator, waitch func() <-chan struct{}) http.Handler {
	return &schedulerHandler{
		queueOperator: q,
		waitch:        waitch,
	}
}

type schedulerHandler struct {
	queueOperator *queueservice.QueueOperator
	waitch        func() <-chan struct{}
}

func (h *schedulerHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method != "POST" {
		http.Error(w, "Method Not Allowed", http.StatusMethodNotAllowed)
		return
	}

	defer r.Body.Close()
	b, err := ioutil.ReadAll(r.Body)
	if err != nil {
		http.Error(w, "error reading body", http.StatusBadRequest)
		return
	}

	var v []byte
	switch r.URL.Path {
	case EvalQueuePrefix:
		lreq := pb.ScheduleEvalAddRequest{}
		if lerr := lreq.Unmarshal(b); lerr != nil {
			http.Error(w, "error unmarshalling request", http.StatusBadRequest)
			return
		}
		select {
		case <-h.waitch():
		case <-time.After(applyTimeout):
			http.Error(w, ErrLeaseHTTPTimeout.Error(), http.StatusRequestTimeout)
			return
		}
		eval := convertEvalFromRequest(&lreq)
		resp, err := h.queueOperator.EvalBrokerQueue(eval)
		if err != nil {
			http.Error(w, err.Error(), http.StatusNotFound)
			return
		}
		v, err = resp.Marshal()
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
	case EvalDequeuePrefix:
		lreq := pb.ScheduleEvalDequeueRequest{}
		if lerr := lreq.Unmarshal(b); lerr != nil {
			http.Error(w, "error unmarshalling request", http.StatusBadRequest)
			return
		}
		select {
		case <-h.waitch():
		case <-time.After(applyTimeout):
			http.Error(w, ErrLeaseHTTPTimeout.Error(), http.StatusRequestTimeout)
			return
		}
		resp, err := h.queueOperator.EvalBrokerDequeue(&lreq)
		if err != nil {
			http.Error(w, err.Error(), http.StatusNotFound)
			return
		}
		v, err = resp.Marshal()
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
	case EvalAckPrefix:
		lreq := pb.ScheduleEvalAckRequest{}
		if lerr := lreq.Unmarshal(b); lerr != nil {
			http.Error(w, "error unmarshalling request", http.StatusBadRequest)
			return
		}
		select {
		case <-h.waitch():
		case <-time.After(applyTimeout):
			http.Error(w, ErrLeaseHTTPTimeout.Error(), http.StatusRequestTimeout)
			return
		}
		err := h.queueOperator.EvalBrokerAck(lreq.Id, lreq.Token)
		if err != nil {
			http.Error(w, err.Error(), http.StatusNotFound)
			return
		}
		resp := &pb.ScheduleEvalAckResponse{
			Header: &pb.ResponseHeader{},
		}
		v, err = resp.Marshal()
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
	case EvalNackPrefix:
		lreq := pb.ScheduleEvalNackRequest{}
		if lerr := lreq.Unmarshal(b); lerr != nil {
			http.Error(w, "error unmarshalling request", http.StatusBadRequest)
			return
		}
		select {
		case <-h.waitch():
		case <-time.After(applyTimeout):
			http.Error(w, ErrLeaseHTTPTimeout.Error(), http.StatusRequestTimeout)
			return
		}
		err := h.queueOperator.EvalBrokerNack(lreq.Id, lreq.Token)
		if err != nil {
			http.Error(w, err.Error(), http.StatusNotFound)
			return
		}
		resp := &pb.ScheduleEvalNackResponse{
			Header: &pb.ResponseHeader{},
		}
		v, err = resp.Marshal()
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
	case PlanAllocationAddPrefix:
		lreq := pb.PlanAllocationEnqueueRequest{}
		if lerr := lreq.Unmarshal(b); lerr != nil {
			http.Error(w, "error unmarshalling request", http.StatusBadRequest)
			return
		}
		select {
		case <-h.waitch():
		case <-time.After(applyTimeout):
			http.Error(w, ErrLeaseHTTPTimeout.Error(), http.StatusRequestTimeout)
			return
		}
		allocs, err := h.queueOperator.AllocationEnqueue(&lreq)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		resp := &pb.PlanAllocationEnqueueResponse{
			Header: &pb.ResponseHeader{},
			Data:   allocs,
		}
		v, err = resp.Marshal()
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
	case QueueDetailPrefix:
		lreq := pb.QueueDetailRequest{}
		if lerr := lreq.Unmarshal(b); lerr != nil {
			http.Error(w, "error unmarshalling request", http.StatusBadRequest)
			return
		}
		select {
		case <-h.waitch():
		case <-time.After(applyTimeout):
			http.Error(w, ErrLeaseHTTPTimeout.Error(), http.StatusRequestTimeout)
			return
		}
		data, err := h.queueOperator.QueueStats(lreq.Type)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		resp := &pb.QueueDetailResponse{
			Header: &pb.ResponseHeader{},
			Data:   data,
		}
		v, err = resp.Marshal()
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
	case QueueJobViewDetailPrefix:
		lreq := pb.QueueJobViewRequest{}
		if lerr := lreq.Unmarshal(b); lerr != nil {
			http.Error(w, "error unmarshalling request", http.StatusBadRequest)
			return
		}
		select {
		case <-h.waitch():
		case <-time.After(applyTimeout):
			http.Error(w, ErrLeaseHTTPTimeout.Error(), http.StatusRequestTimeout)
			return
		}
		data, err := h.queueOperator.QueueJobViewStats(lreq.Namespace)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		resp := &pb.QueueJobViewResponse{
			Header: &pb.ResponseHeader{},
			Data:   data,
		}
		v, err = resp.Marshal()
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
	case QueueForbiddenPrefix:
		lreq := pb.QueueForbiddenRequest{}
		if lerr := lreq.Unmarshal(b); lerr != nil {
			http.Error(w, "error unmarshalling request", http.StatusBadRequest)
			return
		}
		select {
		case <-h.waitch():
		case <-time.After(applyTimeout):
			http.Error(w, ErrLeaseHTTPTimeout.Error(), http.StatusRequestTimeout)
			return
		}
		h.queueOperator.QueueForbidden()
		data, err := h.queueOperator.QueueStatusDetail()
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		resp := &pb.QueueForbiddenResponse{
			Header: &pb.ResponseHeader{},
			Data:   data,
		}
		v, err = resp.Marshal()
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
	case QueueStatusPrefix:
		lreq := pb.QueueStatusRequest{}
		if lerr := lreq.Unmarshal(b); lerr != nil {
			http.Error(w, "error unmarshalling request", http.StatusBadRequest)
			return
		}
		select {
		case <-h.waitch():
		case <-time.After(applyTimeout):
			http.Error(w, ErrLeaseHTTPTimeout.Error(), http.StatusRequestTimeout)
			return
		}
		data, err := h.queueOperator.QueueStatusDetail()
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		resp := &pb.QueueStatusResponse{
			Header: &pb.ResponseHeader{},
			Data:   data,
		}
		v, err = resp.Marshal()
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
	default:
		http.Error(w, fmt.Sprintf("unknown request path %q", r.URL.Path), http.StatusBadRequest)
		return
	}
	w.Header().Set("Content-Type", "application/protobuf")
	w.Write(v)
}

// 入队列
func EvalQueueHTTP(ctx context.Context, eval *domain.Evaluation, url string, rt http.RoundTripper) (*pb.ScheduleEvalAddResponse, error) {
	// will post lreq protobuf to leader
	r := &pb.ScheduleEvalAddRequest{
		Id:          eval.ID,
		Namespace:   eval.Namespace,
		Priority:    uint64(eval.Priority),
		Type:        eval.Type,
		TriggeredBy: eval.TriggeredBy,
		JobId:       eval.JobID,
		PlanId:      eval.PlanID,
		Status:      eval.Status,
	}
	lreq, err := r.Marshal()
	if err != nil {
		return nil, err
	}
	b, err := doRequest(ctx, url, lreq, rt)
	if err != nil {
		return nil, err
	}
	lresp := &pb.ScheduleEvalAddResponse{}
	if err := lresp.Unmarshal(b); err != nil {
		return nil, fmt.Errorf(`lease: %v. data = "%s"`, err, string(b))
	}
	//if lresp.LeaseTimeToLiveResponse.ID != int64(id) {
	//	return nil, fmt.Errorf("lease: renew id mismatch")
	//}
	return lresp, nil
}

// 出队列
// EvalDequeueHTTP renews a dequeue at a given primary server.
func EvalDequeueHTTP(ctx context.Context, schedulers []string, timeout uint64, namespace string, url string, rt http.RoundTripper) (*pb.ScheduleEvalDequeueResponse, error) {
	// will post lreq protobuf to leader
	lreq, err := (&pb.ScheduleEvalDequeueRequest{
		Schedulers: schedulers,
		Timeout:    timeout,
		Namespace:  namespace,
	}).Marshal()
	if err != nil {
		return nil, err
	}
	b, err := doRequest(ctx, url, lreq, rt)
	if err != nil {
		return nil, err
	}
	lresp := &pb.ScheduleEvalDequeueResponse{}
	if err := lresp.Unmarshal(b); err != nil {
		return nil, fmt.Errorf(`lease: %v. data = "%s"`, err, string(b))
	}
	//if lresp.LeaseTimeToLiveResponse.ID != int64(id) {
	//	return nil, fmt.Errorf("lease: renew id mismatch")
	//}
	return lresp, nil
}

func EvalAckHTTP(ctx context.Context, id string, token string, namespace string, url string, rt http.RoundTripper) (*pb.ScheduleEvalAckResponse, error) {
	// will post lreq protobuf to leader
	lreq, err := (&pb.ScheduleEvalAckRequest{
		Id:        id,
		Token:     token,
		Namespace: namespace,
	}).Marshal()
	if err != nil {
		return nil, err
	}
	b, err := doRequest(ctx, url, lreq, rt)
	if err != nil {
		return nil, err
	}
	lresp := &pb.ScheduleEvalAckResponse{}
	if err := lresp.Unmarshal(b); err != nil {
		return nil, fmt.Errorf(`lease: %v. data = "%s"`, err, string(b))
	}
	return lresp, nil
}

func EvalNackHTTP(ctx context.Context, id string, token string, namespace string, url string, rt http.RoundTripper) (*pb.ScheduleEvalNackResponse, error) {
	// will post lreq protobuf to leader
	lreq, err := (&pb.ScheduleEvalNackRequest{
		Id:        id,
		Token:     token,
		Namespace: namespace,
	}).Marshal()
	if err != nil {
		return nil, err
	}
	b, err := doRequest(ctx, url, lreq, rt)
	if err != nil {
		return nil, err
	}
	lresp := &pb.ScheduleEvalNackResponse{}
	if err := lresp.Unmarshal(b); err != nil {
		return nil, fmt.Errorf(`lease: %v. data = "%s"`, err, string(b))
	}
	return lresp, nil
}

func PlanAllocationAddHTTP(ctx context.Context, r *pb.PlanAllocationEnqueueRequest, url string, rt http.RoundTripper) (*pb.PlanAllocationEnqueueResponse, error) {
	// will post lreq protobuf to leader
	lreq, err := (&pb.PlanAllocationEnqueueRequest{
		EvalId:      r.EvalId,
		EvalToken:   r.EvalToken,
		Priority:    r.Priority,
		AllAtOnce:   r.AllAtOnce,
		PlanId:      r.PlanId,
		JobId:       r.JobId,
		Allocations: r.Allocations,
	}).Marshal()
	if err != nil {
		return nil, err
	}
	b, err := doRequest(ctx, url, lreq, rt)
	if err != nil {
		return nil, err
	}
	lresp := &pb.PlanAllocationEnqueueResponse{}
	if err := lresp.Unmarshal(b); err != nil {
		return nil, fmt.Errorf(`lease: %v. data = "%s"`, err, string(b))
	}
	return lresp, nil
}

func QueueDetailHTTP(ctx context.Context, r *pb.QueueDetailRequest, url string, rt http.RoundTripper) (*pb.QueueDetailResponse, error) {
	// will post lreq protobuf to leader
	lreq, err := (&pb.QueueDetailRequest{
		Type: r.Type,
	}).Marshal()
	if err != nil {
		return nil, err
	}
	b, err := doRequest(ctx, url, lreq, rt)
	if err != nil {
		return nil, err
	}
	lresp := &pb.QueueDetailResponse{}
	if err := lresp.Unmarshal(b); err != nil {
		return nil, fmt.Errorf(`lease: %v. data = "%s"`, err, string(b))
	}
	return lresp, nil
}

func QueueJobViewDetailHTTP(ctx context.Context, r *pb.QueueJobViewRequest, url string, rt http.RoundTripper) (*pb.QueueJobViewResponse, error) {
	// will post lreq protobuf to leader
	lreq, err := (&pb.QueueJobViewRequest{
		Namespace: r.Namespace,
	}).Marshal()
	if err != nil {
		return nil, err
	}
	b, err := doRequest(ctx, url, lreq, rt)
	if err != nil {
		return nil, err
	}
	lresp := &pb.QueueJobViewResponse{}
	if err := lresp.Unmarshal(b); err != nil {
		return nil, fmt.Errorf(`lease: %v. data = "%s"`, err, string(b))
	}
	return lresp, nil
}

func QueueForbiddenUpdateHTTP(ctx context.Context, r *pb.QueueForbiddenRequest, url string, rt http.RoundTripper) (*pb.QueueForbiddenResponse, error) {
	// will post lreq protobuf to leader
	lreq, err := (&pb.QueueForbiddenRequest{
		Namespace: r.Namespace,
	}).Marshal()
	if err != nil {
		return nil, err
	}
	b, err := doRequest(ctx, url, lreq, rt)
	if err != nil {
		return nil, err
	}
	lresp := &pb.QueueForbiddenResponse{}
	if err := lresp.Unmarshal(b); err != nil {
		return nil, fmt.Errorf(`lease: %v. data = "%s"`, err, string(b))
	}
	return lresp, nil
}

func QueueStatusDetailHTTP(ctx context.Context, r *pb.QueueStatusRequest, url string, rt http.RoundTripper) (*pb.QueueStatusResponse, error) {
	// will post lreq protobuf to leader
	lreq, err := (&pb.QueueStatusRequest{
		Namespace: r.Namespace,
	}).Marshal()
	if err != nil {
		return nil, err
	}
	b, err := doRequest(ctx, url, lreq, rt)
	if err != nil {
		return nil, err
	}
	lresp := &pb.QueueStatusResponse{}
	if err := lresp.Unmarshal(b); err != nil {
		return nil, fmt.Errorf(`lease: %v. data = "%s"`, err, string(b))
	}
	return lresp, nil
}

// doRequest 实际请求
func doRequest(ctx context.Context, url string, lreq []byte, rt http.RoundTripper) (response []byte, err error) {
	req, err := http.NewRequest("POST", url, bytes.NewReader(lreq))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/protobuf")
	req = req.WithContext(ctx)
	cc := &http.Client{Transport: rt}
	var b []byte
	// buffer errc channel so that errc don't block inside the go routinue
	resp, err := cc.Do(req)
	if err != nil {
		return nil, err
	}
	b, err = readResponse(resp)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode == http.StatusRequestTimeout {
		return nil, ErrLeaseHTTPTimeout
	}
	if resp.StatusCode == http.StatusNotFound {
		return nil, ErrScheduleNotFound
	}
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("scheduler http: unknown error(%s)", string(b))
	}
	return b, nil
}

func readResponse(resp *http.Response) (b []byte, err error) {
	b, err = ioutil.ReadAll(resp.Body)
	httputil.GracefulClose(resp)
	return
}

func convertEvalFromRequest(r *pb.ScheduleEvalAddRequest) *domain.Evaluation {
	if r == nil {
		return &domain.Evaluation{}
	}
	return &domain.Evaluation{
		ID:          r.Id,
		Namespace:   r.Namespace,
		Priority:    int(r.Priority),
		Type:        r.Type,
		TriggeredBy: r.TriggeredBy,
		JobID:       r.JobId,
		PlanID:      r.PlanId,
		Status:      r.Status,
	}
}
