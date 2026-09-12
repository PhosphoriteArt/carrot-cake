FROM rust:1.95-alpine AS builder
RUN apk add perl make ca-certificates

WORKDIR /build
COPY ./ ./
RUN cargo build --release

FROM scratch AS runner
COPY --from=builder /etc/ssl /etc/ssl
WORKDIR /app
COPY --from=builder /build/target/release/carrot_cake ./carrot_cake
STOPSIGNAL SIGINT
ENTRYPOINT [ "/app/carrot_cake" ]