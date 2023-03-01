FROM golang:1.19 as builder

ARG GO_FILE=out_mongo

WORKDIR /go/src

COPY . .
RUN go build -buildmode=c-shared -o /go/bin/out_mongo.so -- *.go

########################################################

FROM fluent/fluent-bit:2.0.8

COPY --from=builder /go/bin/out_mongo.so /out_mongo.so

CMD ["/fluent-bit/bin/fluent-bit", "-c", "/fluent-bit/etc/fluent-bit.conf", "-e", "/out_mongo.so"]