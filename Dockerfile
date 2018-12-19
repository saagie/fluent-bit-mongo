FROM golang:1.11.3-stretch as buildplugin

ARG GO_FILE=out_mongo

COPY $GO_FILE /$GO_FILE
WORKDIR /$GO_FILE


RUN go get github.com/fluent/fluent-bit-go/output
RUN go get github.com/ugorji/go/codec
RUN go build -buildmode=c-shared -o /$GO_FILE.so $GO_FILE.go

########################################################

FROM fluent/fluent-bit:0.14.9 
ARG GO_FILE=out_mongo
ENV PLUGIN_FILE /${GO_FILE}.so

COPY --from=buildplugin $PLUGIN_FILE $PLUGIN_FILE

CMD ["sh","-c","/fluent-bit/bin/fluent-bit -c /fluent-bit/etc/fluent-bit.conf -e $PLUGIN_FILE"]