GOCACHE=$(abspath .gocache)
GOTEST=GOCACHE=$(GOCACHE) go test -v -race 

.PHONY: test
test: 
	bash -c "set -e; set -o pipefail; $(GOTEST) ./..."
