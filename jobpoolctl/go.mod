module yunli.com/jobpool/jobpoolctl/v2

go 1.19

replace (
	yunli.com/jobpool/api/v2 => ../api
	yunli.com/jobpool/client/pkg/v2 => ../client/pkg
	yunli.com/jobpool/client/v2 => ../client/v3
	yunli.com/jobpool/pkg/v2 => ../pkg
	yunli.com/jobpool/server/v2 => ../server
)

// Bad imports are sometimes causing attempts to pull that code.
// This makes the error more explicit.
replace yunli.com/jobpool/v2 => ./FORBIDDEN_DEPENDENCY

require (
	github.com/bgentry/speakeasy v0.1.0
	github.com/dustin/go-humanize v1.0.0
	github.com/olekukonko/tablewriter v0.0.5
	github.com/spf13/cobra v1.1.3
	github.com/spf13/pflag v1.0.5
	go.uber.org/zap v1.17.0
	golang.org/x/time v0.0.0-20210220033141-f8bda1e9f3ba
	google.golang.org/grpc v1.41.0
	gopkg.in/cheggaaa/pb.v1 v1.0.28
	yunli.com/jobpool/api/v2 v2.0.0-00010101000000-000000000000
	yunli.com/jobpool/client/pkg/v2 v2.0.0-00010101000000-000000000000
	yunli.com/jobpool/client/v2 v2.0.0-00010101000000-000000000000
	yunli.com/jobpool/pkg/v2 v2.0.0-00010101000000-000000000000
)

require (
	github.com/coreos/go-semver v0.3.0 // indirect
	github.com/coreos/go-systemd/v22 v22.3.2 // indirect
	github.com/gogo/protobuf v1.3.2 // indirect
	github.com/golang/protobuf v1.5.2 // indirect
	github.com/google/go-cmp v0.5.6 // indirect
	github.com/inconshreveable/mousetrap v1.0.0 // indirect
	github.com/mattn/go-runewidth v0.0.9 // indirect
	go.uber.org/atomic v1.7.0 // indirect
	go.uber.org/multierr v1.6.0 // indirect
	golang.org/x/net v0.7.0 // indirect
	golang.org/x/sys v0.5.0 // indirect
	golang.org/x/text v0.7.0 // indirect
	google.golang.org/genproto v0.0.0-20210602131652-f16073e35f0c // indirect
	google.golang.org/protobuf v1.27.1 // indirect
)
