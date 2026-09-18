<div align="center">
  <img src="https://raw.githubusercontent.com/TypeType-Video/TypeType/main/assets/banner.svg" alt="TypeType" width="100%">
  <h1>TypeType Downloader</h1>
  <p>The native download and muxing service for TypeType.</p>
</div>

You want to know the current position of TypeType about AI ? Go check [this](https://github.com/TypeType-Video/TypeType/blob/dev/AI_TRANSPARENCY.md).

TypeType-Downloader receives jobs from [TypeType-Server](https://github.com/TypeType-Video/TypeType-Server), downloads the selected media streams, muxes audio and video without re-encoding, and publishes the finished artifact through local or S3-compatible storage.

Extraction stays in TypeType-Server. If you want to install or update a complete instance, use the [central TypeType stack](https://github.com/TypeType-Video/TypeType).

## Responsibilities

- Persistent asynchronous download jobs
- Concurrent HTTP Range downloads and progress aggregation
- TypeType SABR segment downloads
- Audio and video stream-copy muxing through libavformat
- SSE job updates with polling-compatible state
- Cancellation, cleanup, and storage-capacity safeguards
- Local filesystem and S3/Garage artifact storage

## Job API

| Method | Path | Purpose |
| --- | --- | --- |
| `GET` | `/health` | Basic service health |
| `GET` | `/health/deep` | Dependency and storage health |
| `POST` | `/jobs` | Create a download job |
| `GET` | `/jobs/{id}` | Read job state and progress |
| `GET` | `/jobs/{id}/events` | Stream job updates through SSE |
| `GET` | `/jobs/{id}/artifact` | Serve or redirect to the artifact |
| `POST` | `/jobs/{id}/cancel` | Cancel a queued or running job |
| `DELETE` | `/jobs/{id}` | Delete a non-running job and its artifact |

Jobs use `queued`, `running`, `done`, and `failed` as their stable lifecycle states. TypeType-Server exposes the authenticated user-facing gateway for this API.

## Stack

| Role | Technology |
| --- | --- |
| Language | Go 1.26 |
| HTTP API | Go standard library |
| Muxing | libavformat through cgo |
| Persistence | PostgreSQL |
| Cache | Dragonfly through the Redis protocol |
| Storage | Local filesystem or S3-compatible Garage |

## Development

Requirements:

- Go 1.26
- A C toolchain
- libavformat, libavcodec, and libavutil development libraries
- Docker Engine and Docker Compose v2 for the local service stack

```sh
gofmt -w cmd internal
go test ./...
go build ./...
```

Run the local Compose stack with development credentials:

```sh
DB_PASSWORD=change-me \
S3_ACCESS_KEY=change-me \
S3_SECRET_KEY=change-me \
docker compose up -d --build
```

## Project structure

| Path | Responsibility |
| --- | --- |
| `cmd` | Service entry points |
| `internal/api` and `internal/server` | HTTP routes and server lifecycle |
| `internal/downloader` and `internal/sabr` | Direct and SABR media transfer |
| `internal/selector` and `internal/pipeline` | Stream selection and job execution |
| `internal/mux` and `internal/ffmpeg` | libavformat integration |
| `internal/storage` and `internal/artifact` | Artifact persistence and delivery |
| `internal/db`, `internal/job`, and `migrations` | Job persistence |

Read [CONTRIBUTING.md](CONTRIBUTING.md) before opening a pull request. Bug reports and feature requests belong in the [central issue tracker](https://github.com/TypeType-Video/TypeType/issues).

## License

TypeType-Downloader is licensed under [GPL-3.0-or-later](LICENSE). Runtime notices are listed in [THIRD_PARTY_NOTICES.md](THIRD_PARTY_NOTICES.md).
