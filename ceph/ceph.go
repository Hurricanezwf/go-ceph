package ceph

import "fmt"

type Request interface {
	Do(p *RequestParam) Response
}

type Response interface {
	Err() error
}

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
	c.IP = ip
}

func (c *Ceph) SetPort(port int) {
	c.Port = port
}

func (c *Ceph) SetAccessKey(k string) {
	c.AccessKey = k
}

func (c *Ceph) SetSecretKey(k string) {
	c.SecretKey = k
}

func (c *Ceph) Do(r Request) Response {
	p := &RequestParam{
		Host:      fmt.Sprintf("%s:%d", c.IP, c.Port),
		AccessKey: c.AccessKey,
		SecretKey: c.SecretKey,
	}
	return r.Do(p)
}
