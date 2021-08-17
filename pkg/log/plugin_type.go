package log

import "fmt"

//go:generate stringer -type=PluginType -linecomment
type PluginType int

const (
	InputPlugin  PluginType = iota // input
	FilterPlugin                   // filter
	OutputPlugin                   // output
)

var _ fmt.Stringer = PluginType(0)
