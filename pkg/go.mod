module yunli.com/jobpool/pkg/v2

go 1.19

require (
	github.com/creack/pty v1.1.11
	github.com/dustin/go-humanize v1.0.0
	github.com/oklog/ulid/v2 v2.1.0
	github.com/spf13/cobra v1.1.3
	github.com/spf13/pflag v1.0.5
	github.com/stretchr/testify v1.8.1
	go.uber.org/zap v1.17.0
	golang.org/x/sys v0.0.0-20210403161142-5e06dd20ab57
	google.golang.org/grpc v1.41.0
	yunli.com/jobpool/client/pkg/v2 v2.0.0-00010101000000-000000000000
)

require (
	github.com/davecgh/go-spew v1.1.1 // indirect
	github.com/golang/protobuf v1.5.1 // indirect
	github.com/inconshreveable/mousetrap v1.0.0 // indirect
	github.com/pmezard/go-difflib v1.0.0 // indirect
	go.uber.org/atomic v1.7.0 // indirect
	go.uber.org/multierr v1.6.0 // indirect
	golang.org/x/net v0.0.0-20200822124328-c89045814202 // indirect
	golang.org/x/text v0.3.2 // indirect
	google.golang.org/genproto v0.0.0-20200526211855-cb27e3aa2013 // indirect
	google.golang.org/protobuf v1.26.0 // indirect
	gopkg.in/yaml.v3 v3.0.1 // indirect
)

replace (
	yunli.com/jobpool => ./FORBIDDEN_DEPENDENCY
	yunli.com/jobpool/client/pkg/v2 => ../client/pkg
	yunli.com/jobpool/v2 => ./FORBIDDEN_DEPENDENCY
)
