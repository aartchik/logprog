FROM golang:1.25-alpine AS build

ARG TARGETARCH

WORKDIR /go/src/proglog

COPY . .

RUN CGO_ENABLED=0 go build -o /go/bin/proglog ./cmd/proglog

RUN GRPC_HEALTH_PROBE_VERSION=v0.3.2 && \
    wget -qO /go/bin/grpc_health_probe \
    https://github.com/grpc-ecosystem/grpc-health-probe/releases/download/${GRPC_HEALTH_PROBE_VERSION}/grpc_health_probe-linux-${TARGETARCH} && \
    chmod +x /go/bin/grpc_health_probe

FROM scratch

COPY --from=build /go/bin/proglog /bin/proglog
COPY --from=build /go/bin/grpc_health_probe /bin/grpc_health_probe

ENTRYPOINT ["/bin/proglog"]
