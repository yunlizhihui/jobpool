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

package rest

import (
	"context"
	"encoding/json"
	"fmt"
	resty "github.com/go-resty/resty/v2"
	"go.uber.org/zap"
	"strings"
	"time"
	"yunli.com/jobpool/agent/v2/plugins"
	"yunli.com/jobpool/api/v2/constant"
	"yunli.com/jobpool/api/v2/domain"
)

type Plugin struct {
	ctx       context.Context
	namespace string
	config    map[string]interface{}
	logger    *zap.Logger
	client    *resty.Client
}

func NewPlugin(ctx context.Context, logger *zap.Logger) plugins.PluginRunner {
	restClient := resty.New()
	restClient.
		// Set retry count to non zero to enable retries
		SetRetryCount(3).
		// You can override initial retry wait time.
		// Default is 100 milliseconds.
		SetRetryWaitTime(5 * time.Second).
		// MaxWaitTime can be overridden as well.
		// Default is 2 seconds.
		SetRetryMaxWaitTime(20 * time.Second)
	return &Plugin{
		ctx:    ctx,
		logger: logger,
		client: restClient,
	}
}

func NewDispatcher(ctx context.Context, logger *zap.Logger) plugins.PluginJobDispatcher {
	restClient := resty.New()
	restClient.
		// Set retry count to non zero to enable retries
		SetRetryCount(3).
		// You can override initial retry wait time.
		// Default is 100 milliseconds.
		SetRetryWaitTime(5 * time.Second).
		// MaxWaitTime can be overridden as well.
		// Default is 2 seconds.
		SetRetryMaxWaitTime(20 * time.Second)
	return &Plugin{
		ctx:    ctx,
		logger: logger,
		client: restClient,
	}
}

// 同步任务
func (p Plugin) StartSynchroJob(request *plugins.JobRequest) (*plugins.JobResult, error) {
	var result plugins.JobResult
	// 拼接参数直接调用返回状态
	if request == nil || request.Setting == nil {
		return nil, domain.NewErr1001Blank("请求参数")
	}
	var restSetting RestSetting
	err := json.Unmarshal(request.Setting, &restSetting)
	if err != nil {
		p.logger.Warn("un marshal setting config failed", zap.Error(err))
		result.State = constant.JobStatusFailed
		result.Message = fmt.Sprintf("un marshal setting config failed: %s", err.Error())
		return &result, err
	}
	if restSetting.ContentType == "" {
		restSetting.ContentType = "application/json"
	}
	resp, err := p.sendRequest(&restSetting)
	if err != nil {
		p.logger.Warn("send request to url failed", zap.String("url", restSetting.Url), zap.Error(err))
		result.State = constant.JobStatusFailed
		result.Message = fmt.Sprintf("send request to url failed: %s", err.Error())
		return &result, err
	}

	if resp.StatusCode() == 200 {
		result.State = constant.JobStatusComplete
	} else {
		result.State = constant.JobStatusFailed
	}
	return &result, nil
}

// 异步任务，请求成功将状态更改为running，后续状态上报后更改为完成
func (p Plugin) StartAsynchroJob(request *plugins.JobRequest) (*plugins.JobResult, error) {
	var result plugins.JobResult
	// 拼接参数直接调用返回状态
	if request == nil || request.Setting == nil {
		return nil, domain.NewErr1001Blank("请求参数")
	}
	var restSetting RestSetting
	err := json.Unmarshal(request.Setting, &restSetting)
	if err != nil {
		p.logger.Warn("un marshal setting config failed", zap.Error(err))
		result.State = constant.JobStatusFailed
		result.Message = fmt.Sprintf("un marshal setting config failed: %s", err.Error())
		return &result, err
	}
	if restSetting.ContentType == "" {
		restSetting.ContentType = "application/json"
	}
	if len(request.Params) > 0 && request.Params["body"] != "" {
		restSetting.Body = request.Params["body"]
	}
	resp, err := p.sendRequest(&restSetting)
	if err != nil {
		p.logger.Warn("send request to url failed", zap.String("url", restSetting.Url), zap.Error(err))
		result.State = constant.JobStatusFailed
		result.Message = fmt.Sprintf("send request to url failed: %s", err.Error())
		return &result, err
	}
	if resp.StatusCode() == 200 {
		var respDto *ResponseDto
		if resp.Body() != nil {
			if err := json.Unmarshal(resp.Body(), &respDto); err != nil {
				p.logger.Warn("unmarshal body error ", zap.String("body", string(resp.Body())), zap.Error(err))
				result.State = constant.JobStatusRunning
			} else {
				if respDto.Code == 200 {
					result.State = constant.JobStatusRunning
				} else if respDto.Error != "" {
					p.logger.Warn("request send error ", zap.String("body", string(resp.Body())), zap.String("error", respDto.Error))
					result.State = constant.JobStatusFailed
					result.Message = respDto.Error
				}
			}
		} else {
			result.State = constant.JobStatusRunning
		}

	} else {
		result.State = constant.JobStatusFailed
	}
	return &result, nil
}

func (p Plugin) sendRequest(restSetting *RestSetting) (*resty.Response, error) {
	var resp *resty.Response
	var err error
	if "get" == strings.ToLower(restSetting.Method) {
		resp, err = p.client.R().
			SetHeader("Accept", restSetting.ContentType).
			Get(restSetting.Url)
	} else if "post" == strings.ToLower(restSetting.Method) {
		resp, err = p.client.R().
			SetHeader("Content-Type", restSetting.ContentType).
			SetBody(restSetting.Body).
			Post(restSetting.Url)

	} else if "delete" == strings.ToLower(restSetting.Method) {
		resp, err = p.client.R().
			SetHeader("Content-Type", restSetting.ContentType).
			SetBody(restSetting.Body).
			Delete(restSetting.Url)
	}
	return resp, err
}

// DispatchSkipJob 跳过任务
func (p Plugin) DispatchSkipJob(planId string, jobId string, parameter []byte) error {
	var restSetting RestSetting
	err := json.Unmarshal(parameter, &restSetting)
	if err != nil {
		p.logger.Warn("un marshal setting config failed", zap.Error(err))
		return err
	}
	if restSetting.ContentType == "" {
		restSetting.ContentType = "application/json"
	}
	body := fmt.Sprintf(`{"planId": "%s", "jobId":"%s", "status": "%s", "message":"same job is running, skipped"}`, planId, jobId, constant.JobStatusSkipped)
	restSetting.Body = body
	resp, err := p.sendRequest(&restSetting)
	if err != nil {
		p.logger.Warn("send request to url failed", zap.String("url", restSetting.Url), zap.Error(err))
		return err
	}
	if resp.StatusCode() == 200 {
		p.logger.Debug("dispatch skip job success", zap.String("planId", planId))
	} else {
		p.logger.Debug("dispatch skip job failed", zap.String("planId", planId))
	}
	return nil
}

// 无槽位失败任务
func (p Plugin) DispatchNoSlotJob(planId string, jobId string, parameter []byte) error {
	var restSetting RestSetting
	err := json.Unmarshal(parameter, &restSetting)
	if err != nil {
		p.logger.Warn("un marshal setting config failed", zap.Error(err))
		return err
	}
	if restSetting.ContentType == "" {
		restSetting.ContentType = "application/json"
	}
	body := fmt.Sprintf(`{"planId": "%s", "jobId":"%s", "status": "%s", "message":"no slot left in scheduler engine"}`, planId, jobId, constant.JobStatusFailed)
	restSetting.Body = body
	resp, err := p.sendRequest(&restSetting)
	if err != nil {
		p.logger.Warn("send request to url failed", zap.String("url", restSetting.Url), zap.Error(err))
		return err
	}
	if resp.StatusCode() == 200 {
		p.logger.Debug("dispatch no slot job success", zap.String("planId", planId), zap.String("jobId", jobId))
	} else {
		p.logger.Debug("dispatch no slot job failed", zap.String("planId", planId), zap.String("jobId", jobId))
	}
	return nil
}
