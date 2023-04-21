FROM golang:alpine as go
WORKDIR /app
ENV GO111MODULE=on

COPY go.mod .
RUN go mod download

COPY . .
RUN go build -o message-handler ./cmd/messagehandler

FROM alpine

WORKDIR /app

COPY --from=go /app/message-handler ./message-handler
COPY run/config.yaml ./config.yaml
CMD ["./message-handler"]