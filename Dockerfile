FROM golang:1.23 AS builder

WORKDIR /plugin

COPY ./go.mod ./
COPY ./go.sum ./

RUN go mod tidy

COPY . .

RUN go build -trimpath -buildmode=c-shared -o database.so ./plugin.go

FROM fluent/fluent-bit

COPY --from=builder /plugin/database.so /fluent-bit/etc/
COPY ./etc/fluent-bit.conf /fluent-bit/etc/
COPY ./etc/plugins.conf /fluent-bit/etc/

ENTRYPOINT [ "/fluent-bit/bin/fluent-bit" ]
CMD [ "/fluent-bit/bin/fluent-bit", "-c", "/fluent-bit/etc/fluent-bit.conf" ]