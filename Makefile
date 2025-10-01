run: 
	@go run *.go

clear-test:
	@rm -rf ./test

clear-run: clear-test run