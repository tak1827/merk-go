install:
	go get -v -d ./merk/.

fmt:
	go fmt ./merk/
	go vet ./merk/

test:
	go test ./... -v -race -count=10

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

fuzz:
	rm -rf crashers/ && rm -rf corpus/ && rm -rf suppressions/ && rm merk-fuzz.zip
	cd merk
	go-fuzz-build
	go-fuzz -bin=./merk-fuzz.zip
