package ceph

import "fmt"

type Ceph struct {
	IP   string
	Port int

	AccessKey string
	SecretKey string
}

func NewCeph(ip string, port int, accessKey, secretKey string) *Ceph {
	return &Ceph{
		IP:        ip,
		Port:      port,
		AccessKey: accessKey,
		SecretKey: secretKey,
	}
}

func (c *Ceph) SetIP(ip string) {

}

func (c *Ceph) SetPort(port int) {

}

func (c *Ceph) SetAccessKey(k string) {

}

func (c *Ceph) SetSecretKey(k string) {

}

func (c *Ceph) Do(r Request) (Response, error) {
	p := &RequestParam{
		Host:      fmt.Sprintf("%s:%d", c.IP, c.Port),
		AccessKey: c.AccessKey,
		SecretKey: c.SecretKey,
	}
	return r.Do(p)
}
