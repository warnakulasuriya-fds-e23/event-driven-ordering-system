# ---- Builder stage ----------------------------------------------
# Compiles a self-contained static binary using the Go toolchain.
FROM golang:alpine AS builder

# git is required to fetch Go dependencies during `go mod download`.
RUN apk add --no-cache git

WORKDIR /app

# Fetch dependencies first so they are cached across rebuilds.
COPY go.mod go.sum ./
RUN go mod download

# Build the application into a static binary (no CGO/glibc dependencies).
COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -o app

# ---- Runtime stage ----------------------------------------------
# The binary is fully static, so it runs on an empty image.
FROM scratch

COPY --from=builder /app/app /app/app

ENTRYPOINT ["/app/app"]