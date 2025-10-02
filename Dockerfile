FROM golang:1.25.1-alpine3.22 AS prebuild

RUN apk add --no-cache make

WORKDIR /go/src/app

COPY go.mod go.sum ./
RUN go mod download

COPY . .

RUN go build -o /go/bin/godel ./cmd/godel

FROM alpine:3.22

WORKDIR /usr/src/app
COPY --from=prebuild /go/bin/godel ./godel

CMD ["./godel", "server"]
