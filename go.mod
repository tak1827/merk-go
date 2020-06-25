module github.com/tak1827/merk-go

go 1.14

replace github.com/go-interpreter/wagon => github.com/perlin-network/wagon v0.3.1-0.20180825141017-f8cb99b55a39

require (
	github.com/dgraph-io/badger/v2 v2.0.3
	github.com/golang/protobuf v1.3.2 // indirect
	github.com/lithdew/bytesutil v0.0.0-20200409052507-d98389230a59
	github.com/minio/highwayhash v1.0.0
	github.com/pkg/errors v0.8.1
	github.com/stretchr/testify v1.5.1
	github.com/valyala/bytebufferpool v1.0.0
	golang.org/x/crypto v0.0.0-20190911031432-227b76d455e7
	golang.org/x/net v0.0.0-20190918130420-a8b05e9114ab // indirect
	golang.org/x/sys v0.0.0-20190919044723-0c1ff786ef13 // indirect
)
