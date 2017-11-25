package ceph

import "time"

// 基于当前主机的时间计算格林尼治时间
func GMTime() string {
	t := time.Now().Local()
	t = t.Add(-8 * time.Hour)
	return t.Format("Sat, 2 Jan 2006 15:04:05 GMT")
}
