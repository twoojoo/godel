run: 
	@LOG_LEVEL=debug go run *.go

clear-test:
	@rm -rf ./test

clear-run: clear-test run