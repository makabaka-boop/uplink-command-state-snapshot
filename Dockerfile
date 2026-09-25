# syntax=docker/dockerfile:1

# ---- 构建阶段 ----
FROM golang:1.23-bookworm AS build
WORKDIR /src

# 先拉依赖以利用层缓存
COPY go.mod go.sum ./
RUN --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    go mod download

COPY . .
RUN --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    CGO_ENABLED=0 GOOS=linux go build \
        -trimpath -ldflags="-s -w" \
        -o /out/api ./cmd/api \
    && CGO_ENABLED=0 GOOS=linux go build \
        -trimpath -ldflags="-s -w" \
        -o /out/verify ./cmd/verify

# ---- 运行阶段：distroless 非 root ----
FROM gcr.io/distroless/base-debian12:nonroot
WORKDIR /app
COPY --from=build /out/api /app/api
COPY --from=build /out/verify /app/verify
# 不设 ENTRYPOINT：compose 分别以 /app/api、/app/verify 启动。
