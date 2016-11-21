// +build !linux

package cgroup

import (
	"github.com/moonfrog/telegraf"
)

func (g *CGroup) Gather(acc telegraf.Accumulator) error {
	return nil
}
