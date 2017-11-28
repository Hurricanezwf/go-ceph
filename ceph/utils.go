package ceph

import (
	"crypto/md5"
	"encoding/base64"
	"errors"
	"io"
	"os"
	"time"
)

// 基于当前主机的时间计算格林尼治时间
func GMTime() string {
	t := time.Now().Local()
	t = t.Add(-8 * time.Hour)
	return t.Format("Sat, 2 Jan 2006 15:04:05 GMT")
}

func Base64MD5(f *os.File) (string, error) {
	if f == nil {
		return "", errors.New("Bad file")
	}

	f.Seek(0, os.SEEK_SET)

	h := md5.New()
	if _, err := io.Copy(h, f); err != nil {
		return "", err
	}

	b64 := base64.StdEncoding.EncodeToString(h.Sum(nil))

	return b64, nil
}
