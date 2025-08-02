package utils

import (
	"github.com/OpenListTeam/OpenList/v4/internal/conf"
	"github.com/OpenListTeam/OpenList/v4/pkg/singleflight"
	"github.com/shirou/gopsutil/v4/mem"
)

var (
	maxBufferLimit = 16 * MB
)

func updateMaxBufferLimit() (error, error) {
	m, err := mem.VirtualMemory()
	if err != nil {
		return nil, nil
	}
	maxBufferLimit = int(min(float64(m.Total)*0.05, float64(m.Available)*0.1))
	maxBufferLimit = max(maxBufferLimit, 4*MB)
	return nil, nil
}

func MaxBufferLimit() int {
	if conf.Conf.MaxBufferLimit >= 0 {
		return conf.Conf.MaxBufferLimit
	}
	_, _, _ = singleflight.ErrorGroup.Do("updateMaxBufferLimit", updateMaxBufferLimit)
	return maxBufferLimit
}
