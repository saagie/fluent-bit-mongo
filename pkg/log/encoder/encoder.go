package encoder

import (
	"encoding/base64"
	"fmt"
	"sync"
	"time"

	"go.uber.org/zap/buffer"
	"go.uber.org/zap/zapcore"
)

type Encoder struct {
	*buffer.Buffer

	config     zapcore.EncoderConfig
	namespaces []string
}

var (
	pool     buffer.Pool
	poolInit sync.Once
)

func New(config zapcore.EncoderConfig) (zapcore.Encoder, error) {
	poolInit.Do(func() {
		pool = buffer.NewPool()
	})

	return &Encoder{
		Buffer: pool.Get(),
		config: config,
	}, nil
}

func (enc *Encoder) Clone() zapcore.Encoder {
	return enc.clone()
}

func (enc *Encoder) clone() *Encoder {
	buffer := pool.Get()

	buffer.AppendString(pool.Get().String())

	return &Encoder{
		Buffer: buffer,

		config: enc.config,
	}
}

func (enc *Encoder) EncodeEntry(entry zapcore.Entry, fields []zapcore.Field) (*buffer.Buffer, error) {
	final := enc.clone()

	final.Buffer.AppendTime(entry.Time, "[2006/01/02 15:04:05] ")
	final.AppendString(fmt.Sprintf("[%5s] [", entry.Level))
	final.AppendString(entry.LoggerName)
	final.AppendString("] ")
	final.AppendString(entry.Message)

	for _, f := range fields {
		final.AppendByte(' ')
		f.AddTo(final)
	}

	final.AppendByte('\n')

	return enc.Buffer, nil
}

func (enc *Encoder) AddArray(key string, marshaler zapcore.ArrayMarshaler) error {
	enc.AppendKey(key)
	return marshaler.MarshalLogArray(enc)
}

func (enc *Encoder) AddObject(key string, marshaler zapcore.ObjectMarshaler) error {
	enc.AppendKey(key)
	return marshaler.MarshalLogObject(enc)
}

func (enc *Encoder) AddBinary(key string, val []byte) {
	enc.AddString(key, base64.StdEncoding.EncodeToString(val))
}

func (enc *Encoder) AddByteString(key string, value []byte) {
	enc.AppendKey(key)
	enc.AppendByteString(value)
}

func (enc *Encoder) AddBool(key string, value bool) {
	enc.AppendKey(key)
	enc.AppendBool(value)
}

func (enc *Encoder) AddComplex128(key string, value complex128) {
	enc.AppendKey(key)
	enc.AppendComplex128(value)
}

func (enc *Encoder) AddDuration(key string, value time.Duration) {
	enc.AppendKey(key)
	enc.AppendDuration(value)
}

func (enc *Encoder) AddFloat64(key string, value float64) {
	enc.AppendKey(key)
	enc.AppendFloat64(value)
}

func (enc *Encoder) AddInt64(key string, value int64) {
	enc.AppendKey(key)
	enc.AppendInt64(value)
}

func (enc *Encoder) AddString(key, value string) {
	enc.AppendKey(key)
	enc.AppendString(value)
}

func (enc *Encoder) AddTime(key string, value time.Time) {
	enc.AppendKey(key)
	enc.AppendTime(value)
}

func (enc *Encoder) AddUint64(key string, value uint64) {
	enc.AppendKey(key)
	enc.AppendUint64(value)
}

func (enc *Encoder) AddReflected(key string, value interface{}) error {
	enc.AppendKey(key)
	return enc.AppendReflected(value)
}

func (enc *Encoder) OpenNamespace(key string) {
	enc.namespaces = append(enc.namespaces, key)
}

func (enc *Encoder) AddComplex64(k string, v complex64) { enc.AddComplex128(k, complex128(v)) }
func (enc *Encoder) AddFloat32(k string, v float32)     { enc.AddFloat64(k, float64(v)) }
func (enc *Encoder) AddInt(k string, v int)             { enc.AddInt64(k, int64(v)) }
func (enc *Encoder) AddInt32(k string, v int32)         { enc.AddInt64(k, int64(v)) }
func (enc *Encoder) AddInt16(k string, v int16)         { enc.AddInt64(k, int64(v)) }
func (enc *Encoder) AddInt8(k string, v int8)           { enc.AddInt64(k, int64(v)) }
func (enc *Encoder) AddUint(k string, v uint)           { enc.AddUint64(k, uint64(v)) }
func (enc *Encoder) AddUint32(k string, v uint32)       { enc.AddUint64(k, uint64(v)) }
func (enc *Encoder) AddUint16(k string, v uint16)       { enc.AddUint64(k, uint64(v)) }
func (enc *Encoder) AddUint8(k string, v uint8)         { enc.AddUint64(k, uint64(v)) }
func (enc *Encoder) AddUintptr(k string, v uintptr)     { enc.AddUint64(k, uint64(v)) }
func (enc *Encoder) AppendComplex64(v complex64)        { enc.AppendComplex128(complex128(v)) }
func (enc *Encoder) AppendFloat64(v float64)            { enc.AppendFloat(v, 64) }
func (enc *Encoder) AppendFloat32(v float32)            { enc.AppendFloat(float64(v), 32) }
func (enc *Encoder) AppendInt(v int)                    { enc.Buffer.AppendInt(int64(v)) }
func (enc *Encoder) AppendInt64(v int64)                { enc.AppendInt(int(v)) }
func (enc *Encoder) AppendInt32(v int32)                { enc.AppendInt(int(v)) }
func (enc *Encoder) AppendInt16(v int16)                { enc.AppendInt(int(v)) }
func (enc *Encoder) AppendInt8(v int8)                  { enc.AppendInt(int(v)) }
func (enc *Encoder) AppendUint(v uint)                  { enc.Buffer.AppendUint(uint64(v)) }
func (enc *Encoder) AppendUint64(v uint64)              { enc.AppendUint(uint(v)) }
func (enc *Encoder) AppendUint32(v uint32)              { enc.AppendUint(uint(v)) }
func (enc *Encoder) AppendUint16(v uint16)              { enc.AppendUint(uint(v)) }
func (enc *Encoder) AppendUint8(v uint8)                { enc.AppendUint(uint(v)) }
func (enc *Encoder) AppendUintptr(v uintptr)            { enc.AppendUint(uint(v)) }
func (enc *Encoder) AppendTime(v time.Time)             { enc.Buffer.AppendTime(v, time.RFC3339) }

func (enc *Encoder) AppendKey(key string) {
	for _, namespace := range enc.namespaces {
		enc.AppendString(namespace)
		enc.AppendByte('.')
	}
	enc.AppendString(key)
	enc.AppendByte('=')
}

func (enc *Encoder) AppendArray(marshaler zapcore.ArrayMarshaler) error {
	return marshaler.MarshalLogArray(enc)
}

func (enc *Encoder) AppendObject(marshaler zapcore.ObjectMarshaler) error {
	return marshaler.MarshalLogObject(enc)
}

func (enc *Encoder) AppendReflected(object interface{}) error {
	enc.AppendString(fmt.Sprintf("%+v", object))
	return nil
}

func (enc *Encoder) AppendByteString(value []byte) {
	enc.AppendString(string(value))
}

func (enc *Encoder) AppendComplex128(val complex128) {
	// Cast to a platform-independent, fixed-size type.
	r, i := float64(real(val)), float64(imag(val))
	enc.AppendByte('"')
	// Because we're always in a quoted string, we can use strconv without
	// special-casing NaN and +/-Inf.
	enc.AppendFloat(r, 64)
	enc.AppendByte('+')
	enc.AppendFloat(i, 64)
	enc.AppendByte('i')
	enc.AppendByte('"')
}

func (enc *Encoder) AppendDuration(val time.Duration) {
	enc.AppendString(fmt.Sprintf("%v", val))
}
