module github.com/popododo0720/vws-compute

go 1.26.0

require (
	cloud.google.com/go/longrunning v1.3.0
	github.com/popododo0720/vws-proto v0.0.0
	golang.org/x/crypto v0.55.0
	google.golang.org/genproto/googleapis/rpc v0.0.0-20260928230214-8a89bd6388cc
	google.golang.org/grpc v1.84.0
	google.golang.org/protobuf v1.36.12
	modernc.org/sqlite v1.34.5
)

require google.golang.org/genproto/googleapis/api v0.0.0-20261005182115-fad411399dd8 // indirect

require (
	github.com/dustin/go-humanize v1.0.1 // indirect
	github.com/google/uuid v1.6.0 // indirect
	github.com/mattn/go-isatty v0.0.20 // indirect
	github.com/ncruces/go-strftime v0.1.9 // indirect
	github.com/remyoudompheng/bigfft v0.0.0-20230129092748-24d4a6f8daec // indirect
	golang.org/x/net v0.58.0 // indirect
	golang.org/x/sys v0.47.0 // indirect
	golang.org/x/text v0.41.0 // indirect
	modernc.org/libc v1.55.3 // indirect
	modernc.org/mathutil v1.6.0 // indirect
	modernc.org/memory v1.8.0 // indirect
)

replace github.com/popododo0720/vws-proto => ./third_party/vws-proto
