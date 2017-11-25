package main

import (
	"flag"
	"log"

	"github.com/Hurricanezwf/go-ceph/ceph"
)

var (
	IP        = "ceph1"
	Port      = 7480
	AccessKey = "X0KW4ZDNL5AGVDIZ7QCC"
	SecretKey = "eQtkBx6oIrCYIh8nWgyuXXmQHZnTnDn1Qt1N9DEC"
)

var (
	f      string
	bucket string
)

func init() {
	log.SetFlags(log.LstdFlags | log.Lshortfile)

	flag.StringVar(&f, "f", "", "function name")
	flag.StringVar(&bucket, "b", "", "bucket name")
	flag.Parse()
}

func main() {
	c := ceph.NewCeph(IP, Port, AccessKey, SecretKey)

	switch f {
	case "getallbuckets":
		GetAllBuckets(c)
	case "getbucket":
		GetBucket(c)
	default:
		log.Printf("Unknown function name %s\n", f)
	}
}

func GetAllBuckets(c *ceph.Ceph) {
	r := ceph.NewGetAllBucketsRequest()

	resp, err := c.Do(r)
	if err != nil {
		log.Printf("%v\n", err)
	}
	_ = resp
}

func GetBucket(c *ceph.Ceph) {
	r := ceph.NewGetBucketRequest(bucket, nil)

	resp, err := c.Do(r)
	if err != nil {
		log.Printf("%v\n", err)
	}
	_ = resp

}
