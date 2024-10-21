all:
	rm -rf bin
	go build -buildmode=c-shared -o bin/database.so .

fast:
	go build plugin.go

clean:
	rm -rf ./bin