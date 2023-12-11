module yunli.com/jobpool/agent/v2

go 1.19

replace (
	yunli.com/jobpool/api/v2 => ../api
	yunli.com/jobpool/client/pkg/v2 => ../client/pkg
	yunli.com/jobpool/client/v2 => ../client/v3
	yunli.com/jobpool/pkg/v2 => ../pkg
	yunli.com/jobpool/v2 => ../
)

replace yunli.com/jobpool => ./FORBIDDEN_DEPENDENCY

require (
	github.com/go-resty/resty/v2 v2.7.0
	github.com/gogo/protobuf v1.3.2
	github.com/mattn/go-colorable v0.1.11
	github.com/mitchellh/cli v1.1.5
	go.uber.org/zap v1.17.0
	sigs.k8s.io/yaml v1.2.0
	yunli.com/jobpool/api/v2 v2.0.0-00010101000000-000000000000
	yunli.com/jobpool/client/pkg/v2 v2.0.0-00010101000000-000000000000
	yunli.com/jobpool/client/v2 v2.0.0-00010101000000-000000000000
	yunli.com/jobpool/pkg/v2 v2.0.0-00010101000000-000000000000
	yunli.com/jobpool/v2 v2.0.0-00010101000000-000000000000
)

require (
	github.com/Masterminds/goutils v1.1.1 // indirect
	github.com/Masterminds/semver/v3 v3.1.1 // indirect
	github.com/Masterminds/sprig/v3 v3.2.1 // indirect
	github.com/armon/go-radix v0.0.0-20180808171621-7fddfc383310 // indirect
	github.com/bgentry/speakeasy v0.1.0 // indirect
	github.com/coreos/go-semver v0.3.0 // indirect
	github.com/coreos/go-systemd/v22 v22.3.2 // indirect
	github.com/fatih/color v1.7.0 // indirect
	github.com/golang/protobuf v1.5.2 // indirect
	github.com/google/uuid v1.1.2 // indirect
	github.com/hashicorp/errwrap v1.0.0 // indirect
	github.com/hashicorp/go-multierror v1.0.0 // indirect
	github.com/huandu/xstrings v1.3.2 // indirect
	github.com/imdario/mergo v0.3.11 // indirect
	github.com/kr/text v0.2.0 // indirect
	github.com/mattn/go-isatty v0.0.14 // indirect
	github.com/mitchellh/copystructure v1.0.0 // indirect
	github.com/mitchellh/reflectwalk v1.0.0 // indirect
	github.com/oklog/ulid/v2 v2.1.0 // indirect
	github.com/posener/complete v1.1.1 // indirect
	github.com/robfig/cron/v3 v3.0.1 // indirect
	github.com/shopspring/decimal v1.2.0 // indirect
	github.com/spf13/cast v1.3.1 // indirect
	github.com/spf13/pflag v1.0.5 // indirect
	go.uber.org/atomic v1.7.0 // indirect
	go.uber.org/multierr v1.6.0 // indirect
	golang.org/x/crypto v0.0.0-20220411220226-7b82a4e95df4 // indirect
	golang.org/x/net v0.7.0 // indirect
	golang.org/x/sys v0.5.0 // indirect
	golang.org/x/text v0.7.0 // indirect
	google.golang.org/genproto v0.0.0-20210624195500-8bfb893ecb84 // indirect
	google.golang.org/grpc v1.41.0 // indirect
	google.golang.org/protobuf v1.27.1 // indirect
	gopkg.in/yaml.v2 v2.4.0 // indirect
)
