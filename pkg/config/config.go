package config

import (
	"os"
	"unsafe"

	"github.com/fluent/fluent-bit-go/output"
	mgo "gopkg.in/mgo.v2"
)

func GetAddress(ctx unsafe.Pointer) string {
	return output.FLBPluginConfigKey(ctx, "host_port")
}

func GetUsername(ctx unsafe.Pointer) string {
	return output.FLBPluginConfigKey(ctx, "username")
}

func GetPassword(ctx unsafe.Pointer) string {
	return os.Getenv("MONGOPASSWORD")
}

func GetSource(ctx unsafe.Pointer) string {
	return output.FLBPluginConfigKey(ctx, "auth_database")
}

func GetDatabase(ctx unsafe.Pointer) string {
	return output.FLBPluginConfigKey(ctx, "database")
}

func GetConfig(ctx unsafe.Pointer) *mgo.DialInfo {
	return &mgo.DialInfo{
		Addrs:    []string{GetAddress(ctx)},
		Username: GetUsername(ctx),
		Password: GetPassword(ctx),
		Source:   GetSource(ctx),
		Database: GetDatabase(ctx),
	}
}
