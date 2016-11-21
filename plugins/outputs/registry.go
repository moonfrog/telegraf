package outputs

import (
	"github.com/moonfrog/telegraf"
)

type Creator func() telegraf.Output

var Outputs = map[string]Creator{}

func Add(name string, creator Creator) {
	Outputs[name] = creator
}
