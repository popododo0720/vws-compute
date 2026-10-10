// Package computepb holds the protobuf and gRPC code generated from
// proto/vws/compute/v1/compute.proto. It sits under internal/grpcapi/internal
// so the Go compiler rejects an import from anywhere outside internal/grpcapi.
//
// Regenerate with GOOGLEAPIS pointing at a googleapis checkout (for
// google/rpc/status.proto) and run go generate ./...
package computepb

//go:generate protoc -I ../../../../proto -I ${GOOGLEAPIS} --go_out=../../../.. --go_opt=module=github.com/popododo0720/vws-compute --go-grpc_out=../../../.. --go-grpc_opt=module=github.com/popododo0720/vws-compute vws/compute/v1/compute.proto
