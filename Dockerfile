FROM golang:1.24.6 AS builder

WORKDIR /app
COPY . /app

RUN go mod download
RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 \
    go build -ldflags="-w -s" \
    -gcflags="-B -C" \
    -asmflags="-B" \
    -o writer .

FROM scratch

COPY --from=builder /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/

COPY --from=builder /app/writer /writer

USER 1000:1000

CMD ["/writer"]