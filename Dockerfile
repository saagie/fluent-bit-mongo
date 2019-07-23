FROM golang:1.11.3-stretch as buildplugin

ARG GO_FILE=out_mongo

WORKDIR /$GO_FILE

RUN go get github.com/fluent/fluent-bit-go/output
RUN go get github.com/ugorji/go/codec
RUN go get gopkg.in/mgo.v2
RUN go get github.com/spaolacci/murmur3

COPY $GO_FILE /$GO_FILE
RUN go build -buildmode=c-shared -o /$GO_FILE.so *.go

########################################################

FROM fluent/fluent-bit:1.2.1
ARG GO_FILE=out_mongo
ENV PLUGIN_FILE /${GO_FILE}.so

COPY --from=buildplugin $PLUGIN_FILE $PLUGIN_FILE

CMD ["/fluent-bit/bin/fluent-bit", "-c", "/fluent-bit/etc/fluent-bit.conf","-e","/out_mongo.so"]