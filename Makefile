install:
	go get -v -d ./merk/.

fmt:
	go fmt ./merk/
	go vet ./merk/

test:
	go test ./...

testmerk:
	go test ./merk/

testavl:
	go test ./avl/

bench:
	go test ./... -bench=. -benchtime=10s

benchmerk:
	go test ./merk/ -bench=. -benchtime=5s

benchavl:
	go test ./avl/ -bench=. -benchtime=5s
