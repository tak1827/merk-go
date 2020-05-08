install:
	go get -v -d ./merk/.

fmt:
	go fmt ./merk/
	go vet ./merk/

test:
	go test ./merk/

bench:
	go test ./merk/ -bench=. -benchtime=5s
