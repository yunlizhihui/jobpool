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
	"errors"
	"fmt"
)

const (
	/**
	 * Code定义
	 */
	ERROR_400 = "参数有误"
	ERROR_401 = "无权操作"
	ERROR_500 = "系统异常"

	// 参数校验Code定义：1000 ~ 1099
	ERROR_1001 = "%s不能为空"
	ERROR_1002 = "%s的值只能为[%s]"
	ERROR_1003 = "%s的长度不能超过[%s]"
	ERROR_1004 = "%s[%s]无效"
	ERROR_1005 = "%s不能大于%s[%s，%s]"
	ERROR_1006 = "%s不能重复"
	ERROR_1007 = "%s必须是正整数"
	ERROR_1008 = "%s必须大于%s"
	ERROR_1009 = "%s必须大于等于%s"
	ERROR_1010 = "%s格式不正确[%s]"
	ERROR_1011 = "存在循环依赖，创建失败"

	// 共通业务Code定义：1100 ~ 1199
	ERROR_1100 = "%s[%s]已经存在"
	ERROR_1101 = "%s[%s]不存在"
	ERROR_1102 = "%s[%s]不能重复"
	ERROR_1103 = "结束时间不能小于开始时间"
	ERROR_1104 = "%s[%s]不支持"
	ERROR_1105 = "%s[%s]已被使用,无法删除"
	ERROR_1106 = "%s[%s]是%s,无法%s"
	ERROR_1107 = "执行%s[%s]失败：%s"
	ERROR_1108 = "%s[%s]存在%s,无法删除"
	ERROR_1109 = "%s不存在"

	ErrorNotOutStandingContent   = "evaluation is not outstanding"
	ErrTokenMismatchContent      = "evaluation token does not match"
	ErrNackTimeoutReachedContent = "evaluation nack timeout reached"
)

var (
	Err400WrongParam = errors.New(ERROR_400)
	Err401           = errors.New(ERROR_401)
	Err500           = errors.New(ERROR_500)

	ErrNotOutstanding     = errors.New(ErrorNotOutStandingContent)
	ErrTokenMismatch      = errors.New(ErrTokenMismatchContent)
	ErrNackTimeoutReached = errors.New(ErrNackTimeoutReachedContent)
)

func NewErr1001Blank(name string) error {
	return fmt.Errorf(ERROR_1001, name)
}

func NewErr1002LimitValue(name string, value string) error {
	return fmt.Errorf(ERROR_1002, name, value)
}

func NewErr1003Size(name string, size string) error {
	return fmt.Errorf(ERROR_1003, name, size)
}

func NewErr1004Invalid(name string, value string) error {
	return fmt.Errorf(ERROR_1004, name, value)
}

func NewErr1005Range(name string, value string, start string, end string) error {
	return fmt.Errorf(ERROR_1005, name, value, start, end)
}
func NewErr1006Repeat(name string) error {
	return fmt.Errorf(ERROR_1006, name)
}
func NewErr1007Int(name string) error {
	return fmt.Errorf(ERROR_1007, name)
}
func NewErr1008Above(name string, value string) error {
	return fmt.Errorf(ERROR_1008, name, value)
}

func NewErr1009AboveOn(name string, value string) error {
	return fmt.Errorf(ERROR_1009, name, value)
}

func NewErr1010Format(name string, value string) error {
	return fmt.Errorf(ERROR_1010, name, value)
}

func NewErr1011Circle() error {
	return errors.New(ERROR_1011)
}

func NewErr1100Exist(name string, value string) error {
	return fmt.Errorf(ERROR_1100, name, value)
}

func NewErr1101None(name string, value string) error {
	return fmt.Errorf(ERROR_1101, name, value)
}

func NewErr1102Repeat(name string, value string) error {
	return fmt.Errorf(ERROR_1102, name, value)
}

func NewErr1103StartEnd() error {
	return errors.New(ERROR_1103)
}

func NewErr1104NotSupport(name string, value string) error {
	return fmt.Errorf(ERROR_1104, name, value)
}

func NewErr1105Used(name string, value string) error {
	return fmt.Errorf(ERROR_1105, name, value)
}

func NewErr1106Cannot(name string, value string, real string, espect string) error {
	return fmt.Errorf(ERROR_1106, name, value, real, espect)
}

func NewErr1107FailExecute(name string, value string, msg string) error {
	return fmt.Errorf(ERROR_1107, name, value, msg)
}

func NewErr1108DeleteAble(name string, value string, msg string) error {
	return fmt.Errorf(ERROR_1108, name, value, msg)
}

func NewErr1109NotExist(name string) error {
	return fmt.Errorf(ERROR_1109, name)
}
