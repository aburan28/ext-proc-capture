FROM golang:1.26-alpine AS builder
WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -o /ext-proc-capture .

FROM gcr.io/distroless/static-debian12:nonroot

COPY --from=builder /ext-proc-capture /ext-proc-capture

EXPOSE 9002
ENTRYPOINT ["/ext-proc-capture"]
