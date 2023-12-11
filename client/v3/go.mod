module yunli.com/jobpool/client/v2

go 1.19

replace (
	yunli.com/jobpool/api/v2 => ../../api
	yunli.com/jobpool/client/pkg/v2 => ../pkg
)

// Bad imports are sometimes causing attempts to pull that code.
// This makes the error more explicit.
replace (
	yunli.com/jobpool => ./FORBIDDEN_DEPENDENCY
	yunli.com/jobpool/pkg/v2 => ./FORBIDDEN_DEPENDENCY
	yunli.com/jobpool/v2 => ./FORBIDDEN_DEPENDENCY
)

require (
	github.com/dustin/go-humanize v1.0.0
	go.uber.org/zap v1.17.0
	google.golang.org/grpc v1.41.0
	sigs.k8s.io/yaml v1.2.0
	yunli.com/jobpool/api/v2 v2.0.0-00010101000000-000000000000
	yunli.com/jobpool/client/pkg/v2 v2.0.0-00010101000000-000000000000
)

require (
	github.com/coreos/go-semver v0.3.0 // indirect
	github.com/coreos/go-systemd/v22 v22.3.2 // indirect
	github.com/gogo/protobuf v1.3.2 // indirect
	github.com/golang/protobuf v1.5.2 // indirect
	github.com/kr/pretty v0.1.0 // indirect
	github.com/pkg/errors v0.9.1 // indirect
	go.uber.org/atomic v1.7.0 // indirect
	go.uber.org/multierr v1.6.0 // indirect
	golang.org/x/net v0.0.0-20210405180319-a5a99cb37ef4 // indirect
	golang.org/x/sys v0.0.0-20210603081109-ebe580a85c40 // indirect
	golang.org/x/text v0.3.5 // indirect
	google.golang.org/genproto v0.0.0-20210602131652-f16073e35f0c // indirect
	google.golang.org/protobuf v1.26.0 // indirect
	gopkg.in/check.v1 v1.0.0-20190902080502-41f04d3bba15 // indirect
	gopkg.in/yaml.v2 v2.4.0 // indirect
)
