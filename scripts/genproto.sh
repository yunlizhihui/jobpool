#!/usr/bin/env bash
#
# Generate all etcd protobuf bindings.
# Run from repository root directory named etcd.
#
set -e

if ! [[ "$0" =~ scripts/genproto.sh ]]; then
  echo "must be run from repository root"
  exit 255
fi

source ./scripts/test_lib.sh

# if [[ $(protoc --version | cut -f2 -d' ') != "3.14.0" ]]; then
if [[ $(protoc --version | cut -f2 -d' ') != "24.2" ]]; then
  echo "could not find protoc 3.21.12, is it installed + in PATH?"
  exit 255protoc --version | cut -f2 -d' '
fi

echo "ready:"
GOFAST_BIN=$(tool_get_bin github.com/gogo/protobuf/protoc-gen-gofast)
GRPC_GATEWAY_BIN=$(tool_get_bin github.com/grpc-ecosystem/grpc-gateway/protoc-gen-grpc-gateway)
SWAGGER_BIN=$(tool_get_bin github.com/grpc-ecosystem/grpc-gateway/protoc-gen-swagger)
GOGOPROTO_ROOT="$(tool_pkg_dir github.com/gogo/protobuf/proto)/.."
GRPC_GATEWAY_ROOT="$(tool_pkg_dir github.com/grpc-ecosystem/grpc-gateway/protoc-gen-grpc-gateway)/.."
GRPC_GATEWAY_ROOT2="/Users/david/GoProject/pkg/mod/github.com/grpc-ecosystem/grpc-gateway@v1.16.0"

echo ":"
echo "Resolved binary and packages versions:"
echo "  - protoc-gen-gofast:       ${GOFAST_BIN}"
echo "  - protoc-gen-grpc-gateway: ${GRPC_GATEWAY_BIN}"
echo "  - swagger:                 ${SWAGGER_BIN}"
echo "  - gogoproto-root:          ${GOGOPROTO_ROOT}"
echo "  - grpc-gateway-root:       ${GRPC_GATEWAY_ROOT}"
#GOGOPROTO_PATH="${GOGOPROTO_ROOT}:${GOGOPROTO_ROOT}/protobuf"
GOGOPROTO_PATH="/Users/david/CompanyProject/6scheduler/jobpool/tools/mod/vendor/github.com/gogo/protobuf"

# directories containing protos to be built
# DIRS="./server/wal/walpb ./api/etcdserverpb ./server/etcdserver/api/snap/snappb ./raft/raftpb ./api/mvccpb ./server/lease/leasepb ./api/authpb ./server/etcdserver/api/v3lock/v3lockpb ./server/etcdserver/api/v3election/v3electionpb ./api/membershippb"
DIRS="./api/etcdserverpb ./api/mvccpb ./api/authpb ./api/membershippb ./api/schedulepb"

log_callout -e "\\nRunning gofast (gogo) proto generation..."

for dir in ${DIRS}; do
  run pushd "${dir}"
    run protoc --gofast_out=plugins=grpc:. -I=".:${GOGOPROTO_PATH}:${ETCD_ROOT_DIR}/..:${ETCD_ROOT_DIR}:${GRPC_GATEWAY_ROOT2}/third_party/googleapis" \
      --plugin="${GOFAST_BIN}" ./*.proto

    run sed -i.bak -E 's|"jobpool/api/|"yunli.com/jobpool/api/v2/|g' ./*.pb.go
    run sed -i.bak -E 's|"raft/raftpb"|"go.etcd.io/etcd/raft/v3/raftpb"|g' ./*.pb.go
    run sed -i.bak -E 's|"google.golang.org/protobuf/types/known/timestamppb"|"github.com/gogo/protobuf/types"|g' ./*.pb.go

    rm -f ./*.bak
    run gofmt -s -w ./*.pb.go
#    run goimports -w ./*.pb.go
  run popd
done

log_callout -e "\\nRunning swagger & grpc_gateway proto generation..."

# remove old swagger files so it's obvious whether the files fail to generate
rm -rf doc/dev-guide/apispec/swagger/*json
# for pb in api/etcdserverpb/rpc server/etcdserver/api/v3lock/v3lockpb/v3lock server/etcdserver/api/v3election/v3electionpb/v3election; do
for pb in api/etcdserverpb/rpc; do
  log_callout "grpc & swagger for: ${pb}.proto"
  run protoc -I. \
      -I"${GRPC_GATEWAY_ROOT2}"/third_party/googleapis \
      -I"${GOGOPROTO_PATH}" \
      -I"${ETCD_ROOT_DIR}/.." \
      --grpc-gateway_out=logtostderr=true,paths=source_relative:. \
      --swagger_out=logtostderr=true:./doc/dev-guide/apispec/swagger/. \
      --plugin="${SWAGGER_BIN}" --plugin="${GRPC_GATEWAY_BIN}" \
      ${pb}.proto
  # hack to move gw files around so client won't include them
  pkgpath=$(dirname "${pb}")
  pkg=$(basename "${pkgpath}")
  gwfile="${pb}.pb.gw.go"

  run sed -i -E "s#package $pkg#package gw#g" "${gwfile}"
  # run sed -i -E "s#import \\(#import \\(\"yunli.com/jobpool/${pkgpath}\"#g" "${gwfile}"
  run sed -i -E "s#\"context\"#\"context\"\r\n\t\"yunli.com/jobpool/${pkgpath}\"#g" "${gwfile}"
  run sed -E "s#([ (])([a-zA-Z0-9_]*(Client|Server|Request)([^(]|$))#\\1${pkg}.\\2#g" "${gwfile}" > "${gwfile}.bak"
  run sed -E "s# (New[a-zA-Z0-9_]*Client\\()# ${pkg}.\\1#g" "${gwfile}.bak" > "${gwfile}"
  run sed -i -E "s|yunli.com/jobpool|yunli.com/jobpool/v2|g" "${gwfile}"
  run sed -i -E "s|yunli.com/jobpool/v2/api|yunli.com/jobpool/api/v2|g" "${gwfile}"
#  run sed -i -E "s|yunli.com/jobpool/v2/server|go.etcd.io/etcd/server/v3|g" "${gwfile}"
  
  run go fmt "${gwfile}"

  gwdir="${pkgpath}/gw/"
  run mkdir -p "${gwdir}"
  run mv "${gwfile}" "${gwdir}"
  run rm -f "${gwfile}.bak"
  run rm -f api/etcdserverpb/rpc.pb.gw.go-E

  swaggerName=$(basename ${pb})
  run mv  doc/dev-guide/apispec/swagger/${pb}.swagger.json \
    doc/dev-guide/apispec/swagger/"${swaggerName}".swagger.json
done

log_callout -e "\\nRunning swagger ..."
run_go_tool github.com/hexfusion/schwag -input=doc/dev-guide/apispec/swagger/rpc.swagger.json

if [ "$1" != "--skip-protodoc" ]; then
  log_callout "protodoc is auto-generating grpc API reference documentation..."

  run rm -rf doc/dev-guide/api_reference_v3.md
  run_go_tool yunli.com/protodoc --directories="api/etcdserverpb=service_message,api/mvccpb=service_message,server/lease/leasepb=service_message,api/authpb=service_message" \
    --title="etcd API Reference" \
    --output="doc/dev-guide/api_reference_v3.md" \
    --message-only-from-this-file="api/etcdserverpb/rpc.proto" \
    --disclaimer="This is a generated documentation. Please read the proto files for more." || exit 2

  run rm -rf doc/dev-guide/api_concurrency_reference_v3.md
  run_go_tool yunli.com/protodoc --directories="api/mvccpb=service_message" \
    --title="etcd concurrency API Reference" \
    --output="doc/dev-guide/api_concurrency_reference_v3.md" \
    --disclaimer="This is a generated documentation. Please read the proto files for more." || exit 2

  log_success "protodoc is finished."
else
  log_warning "skipping grpc API reference document auto-generation..."
fi

log_success -e "\\n./genproto SUCCESS"
