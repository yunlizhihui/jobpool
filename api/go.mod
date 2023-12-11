module yunli.com/jobpool/api/v2

go 1.19

require (
	github.com/coreos/go-semver v0.3.0
	github.com/gogo/protobuf v1.3.2
	github.com/golang/protobuf v1.5.2
	github.com/grpc-ecosystem/grpc-gateway v1.16.0
	github.com/oklog/ulid/v2 v2.1.0
	github.com/robfig/cron/v3 v3.0.1
	google.golang.org/genproto v0.0.0-20210602131652-f16073e35f0c
	google.golang.org/grpc v1.41.0
)

require (
	golang.org/x/net v0.0.0-20210405180319-a5a99cb37ef4 // indirect
	golang.org/x/sys v0.0.0-20210510120138-977fb7262007 // indirect
	golang.org/x/text v0.3.5 // indirect
	google.golang.org/protobuf v1.26.0 // indirect
	gopkg.in/yaml.v2 v2.4.0 // indirect
)

// Bad imports are sometimes causing attempts to pull that code.
// This makes the error more explicit.
replace (
	yunli.com/jobpool => ./FORBIDDEN_DEPENDENCY
	yunli.com/jobpool/pkg/v2 => ./FORBIDDEN_DEPENDENCY
	yunli.com/jobpool/v2 => ./FORBIDDEN_DEPENDENCY
)
