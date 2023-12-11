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

package dispatcher

import (
	"context"
	"fmt"
	"go.uber.org/zap"
	"io/ioutil"
	"os"
	"path/filepath"
	"sync"
	"time"
	"yunli.com/jobpool/agent/v2/config"
	"yunli.com/jobpool/agent/v2/service"
	"yunli.com/jobpool/agent/v2/structs"
	"yunli.com/jobpool/api/v2/domain"
	"yunli.com/jobpool/client/pkg/v2/logutil"
	"yunli.com/jobpool/client/pkg/v2/transport"
	clientv3 "yunli.com/jobpool/client/v2"
)

type Dispatcher struct {
	config *config.DispatcherConfig
	start  time.Time
	ctx    context.Context
	cancel context.CancelFunc

	configCopy *config.DispatcherConfig
	configLock sync.RWMutex
	// dispatcher with client
	client *clientv3.Client

	// retry logic
	rpcRetryCh   chan struct{}
	rpcRetryLock sync.Mutex

	// heart beat logic
	// heartbeat related times for tracking how often to heartbeat
	heartbeatTTL    time.Duration
	haveHeartbeated bool
	heartbeatLock   sync.Mutex
	heartbeatStop   *heartbeatStop
	// node
	triggerNodeUpdate chan struct{}
	// allocations
	allocs    map[string]service.AllocRunner
	allocLock sync.RWMutex
	// 如果运行不了，放到invalid中去
	invalidAllocs     map[string]struct{}
	invalidAllocsLock sync.Mutex
	allocUpdates      chan *domain.Allocation

	// 注册成功标记
	hasRegistered bool
	registeredCh  chan struct{}
	registerLock  sync.Mutex

	logger        *zap.Logger
	shutdown      bool
	shutdownCh    chan struct{}
	shutdownLock  sync.Mutex
	shutdownGroup group
}

func NewDispatcher(c *config.DispatcherConfig, log *zap.Logger) (*Dispatcher, error) {
	d := &Dispatcher{
		config:            c,
		start:             time.Now(),
		logger:            log,
		hasRegistered:     false,
		registeredCh:      make(chan struct{}),
		shutdownCh:        make(chan struct{}),
		triggerNodeUpdate: make(chan struct{}, 8),
		allocs:            make(map[string]service.AllocRunner),
		allocUpdates:      make(chan *domain.Allocation, 64),
		invalidAllocs:     make(map[string]struct{}),
	}
	if err := d.init(); err != nil {
		return nil, fmt.Errorf("failed to initialize client: %v", err)
	}
	d.ctx, d.cancel = context.WithCancel(context.Background())
	// init node
	if err := d.setupNode(); err != nil {
		return nil, err
	}
	d.configLock.Lock()
	d.configCopy = d.config.Copy()
	d.configLock.Unlock()
	// 下面是进行一系列处理
	// heartbeat stop
	go d.batchFirstFingerprints()
	d.heartbeatStop = newHeartbeatStop(50*time.Second, d.logger, d.shutdownCh)
	go d.heartbeatStop.watch()

	// 初始化client
	d.configLock.RLock()
	if err := d.initClient(); err != nil {
		return nil, err
	}
	d.configLock.RUnlock()
	// 节点注册&心跳
	d.shutdownGroup.Go(d.registerAndHeartbeat)

	// 当有alloc更新的时候，执行对应的更新操作
	d.shutdownGroup.Go(d.allocSync)

	// run
	go d.run()
	return d, nil
}

func (d *Dispatcher) init() error {
	if d.config.StateDir != "" {
		if err := os.MkdirAll(d.config.StateDir, 0700); err != nil {
			return fmt.Errorf("failed creating state dir: %s", err)
		}
	} else {
		// Otherwise make a temp directory to use.
		p, err := ioutil.TempDir("", "JobpoolClient")
		if err != nil {
			return fmt.Errorf("failed creating temporary directory for the StateDir: %v", err)
		}

		p, err = filepath.EvalSymlinks(p)
		if err != nil {
			return fmt.Errorf("failed to find temporary directory for the StateDir: %v", err)
		}
		d.config.StateDir = p
	}
	return nil
}

func (d *Dispatcher) run() {
	allocUpdates := make(chan *structs.AllocUpdates, 8)
	go d.watchAllocations(allocUpdates)
	for {
		select {
		case update := <-allocUpdates:
			// Don't apply updates while shutting down.
			d.shutdownLock.Lock()
			if d.shutdown {
				d.shutdownLock.Unlock()
				return
			}
			// c.logger.Debug("run the client which begin run allocs----")
			// Apply updates inside lock to prevent a concurrent
			// shutdown.
			d.runAllocations(update)
			d.shutdownLock.Unlock()

		case <-d.shutdownCh:
			return
		}
	}
}

func (d *Dispatcher) Shutdown() error {
	d.shutdownLock.Lock()
	defer d.shutdownLock.Unlock()
	if d.shutdown {
		d.logger.Info("already shutdown")
		return nil
	}
	d.logger.Info("start client shutting down")
	d.shutdown = true
	close(d.shutdownCh)
	err := d.client.Close()
	if err != nil {
		d.logger.Warn("close client failed", zap.Error(err))
	}
	d.shutdownGroup.Wait()
	return nil
}

func (d *Dispatcher) GetConfig() *config.DispatcherConfig {
	return d.config
}

func (d *Dispatcher) Leave() error {

	return nil
}

func (d *Dispatcher) initClient() error {
	if d.config.AdvertiseServerUrls.Len() == 0 {
		return fmt.Errorf("the advertise server url is null")
	}
	// init the client for worker
	cfg := clientv3.Config{
		Endpoints:        d.config.AdvertiseServerUrls.StringSlice(),
		AutoSyncInterval: 0,
		DialTimeout:      5 * time.Second,
	}
	if d.config.ClientSecurityJSON != nil && d.config.ClientSecurityJSON.CertAuth {
		var cfgtls *transport.TLSInfo
		tlsinfo := transport.TLSInfo{}
		tlsinfo.Logger, _ = logutil.CreateDefaultZapLogger(zap.InfoLevel)
		if d.config.ClientSecurityJSON.CertFile != "" {
			tlsinfo.CertFile = d.config.ClientSecurityJSON.CertFile
			cfgtls = &tlsinfo
		}

		if d.config.ClientSecurityJSON.KeyFile != "" {
			tlsinfo.KeyFile = d.config.ClientSecurityJSON.KeyFile
			cfgtls = &tlsinfo
		}

		if d.config.ClientSecurityJSON.TrustedCAFile != "" {
			tlsinfo.TrustedCAFile = d.config.ClientSecurityJSON.TrustedCAFile
			cfgtls = &tlsinfo
		}
		clientTLS, err := cfgtls.ClientConfig()
		if err != nil {
			return err
		}
		cfg.TLS = clientTLS
	}
	if d.config.AuthConfig != nil {
		cfg.Username = d.config.AuthConfig.Username
		cfg.Password = d.config.AuthConfig.Password
	}

	d.logger.Debug("the config", zap.Strings("end", cfg.Endpoints))
	client, err := clientv3.New(cfg)
	if err != nil {
		d.logger.Warn("init client in worker error", zap.Error(err))
	}
	d.client = client
	return nil
}

type group struct {
	wg sync.WaitGroup
}

func (g *group) Wait() {
	g.wg.Wait()
}

// Go starts f in a goroutine and must be called before Wait.
func (g *group) Go(f func()) {
	g.wg.Add(1)
	go func() {
		defer g.wg.Done()
		f()
	}()
}

func (g *group) AddCh(ch <-chan struct{}) {
	g.Go(func() {
		<-ch
	})
}
