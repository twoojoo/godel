build:
	@go build -o bin/godel cmd/godel/*.go

build-image:
	@docker build -t godel:${tag} .

