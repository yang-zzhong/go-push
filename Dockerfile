FROM golang:1.22.2-alpine AS builder


# Install dependencies
# RUN apk --update --no-cache add ca-certificates make wget unzip gcc ffmpeg-dev musl-dev
RUN apk --update --no-cache add ca-certificates make gcc musl-dev

# Set Go env
ENV CGO_ENABLED=1 GOOS=linux
ENV CGO_CFLAGS="-D_LARGEFILE64_SOURCE"
ENV GOPROXY=https://goproxy.cn
WORKDIR /go/src

COPY . .

RUN make build

# Deployment container
FROM alpine

# RUN apk add --update --no-cache ffmpeg-dev

COPY --from=builder /etc/ssl/certs /etc/ssl/certs
COPY --from=builder /go/src/build/push /push

ENTRYPOINT [ "/conn", "--yaml", "/etc/push/config.yaml" ]
