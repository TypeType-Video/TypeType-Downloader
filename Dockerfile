FROM golang:1.27-alpine3.23 AS build

ARG BUILD_VERSION=1.7.0-dev
ARG BUILD_REVISION=development
ARG BUILD_TIME=unknown
WORKDIR /src
RUN apk add --no-cache build-base pkgconf ffmpeg-dev

COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN CGO_ENABLED=1 go build -trimpath -ldflags="-s -w -X typetype-downloader-go/internal/buildinfo.Version=$BUILD_VERSION -X typetype-downloader-go/internal/buildinfo.Revision=$BUILD_REVISION -X typetype-downloader-go/internal/buildinfo.BuildTime=$BUILD_TIME" -o /out/typetype-downloader-go ./cmd/server

FROM alpine:3.23

RUN apk add --no-cache ca-certificates ffmpeg

RUN addgroup -S typetype && adduser -S -G typetype -h /app typetype
WORKDIR /app
COPY --from=build /out/typetype-downloader-go /usr/local/bin/typetype-downloader-go
RUN mkdir -p /app/data && chown -R typetype:typetype /app

USER typetype
EXPOSE 18093
ENTRYPOINT ["/usr/local/bin/typetype-downloader-go"]
