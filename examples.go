package main

import (
	"flag"
	"log"
	"reflect"

	"github.com/Hurricanezwf/go-ceph/ceph"
)

var (
	IP        = "ceph1"
	Port      = 7480
	AccessKey = "X0KW4ZDNL5AGVDIZ7QCC"
	SecretKey = "eQtkBx6oIrCYIh8nWgyuXXmQHZnTnDn1Qt1N9DEC"
)

var (
	f        string // 函数名
	bucket   string // bucket名字
	filePath string // 文件路径
	objName  string
)

var funcMap map[string]func(c *ceph.Ceph)

func init() {
	funcMap = make(map[string]func(c *ceph.Ceph))
	funcMap["getallbuckets"] = GetAllBuckets
	funcMap["getbucket"] = GetBucket
	funcMap["putobj"] = PutObj
}

func main() {
	log.SetFlags(log.LstdFlags | log.Lshortfile)

	flag.StringVar(&f, "f", "", "function name")
	flag.StringVar(&bucket, "b", "", "bucket name")
	flag.StringVar(&filePath, "fp", "", "file path to be put")
	flag.StringVar(&objName, "o", "", "object name")
	flag.Parse()

	fc, ok := funcMap[f]
	if !ok {
		log.Printf("Unknown function name %s\n", f)
		return
	}

	fc(ceph.NewCeph(IP, Port, AccessKey, SecretKey))
}

func GetAllBuckets(c *ceph.Ceph) {
	req := ceph.NewGetAllBucketsRequest()
	resp, err := c.Do(req)
	if err != nil {
		log.Printf("%v\n", err)
		return
	}

	gabresp, ok := resp.(*ceph.GetAllBucketsResponse)
	if !ok {
		log.Printf("Invalid response type, type is %v", reflect.TypeOf(resp))
		return
	}

	log.Println("GetAllBuckets result:")
	for idx, b := range gabresp.Buckets.BucketList {
		log.Printf("(%d) %s\n", idx, b.Name)
	}
}

func GetBucket(c *ceph.Ceph) {
	req := ceph.NewGetBucketRequest(bucket)
	//req.SetOption(nil)
	req.SetValidate(true)
	resp, err := c.Do(req)
	if err != nil {
		log.Printf("%v\n", err)
		return
	}

	gbresp, ok := resp.(*ceph.GetBucketResponse)
	if !ok {
		log.Printf("Invalid response type, type is %v", reflect.TypeOf(resp))
		return
	}

	log.Println("GetBucket result:")
	log.Printf("Name        : %s\n", gbresp.Name)
	log.Printf("Prefix      : %s\n", gbresp.Prefix)
	log.Printf("Marker      : %s\n", gbresp.Marker)
	log.Printf("MaxKeys     : %d\n", gbresp.MaxKeys)
	log.Printf("IsTruncated : %v\n", gbresp.IsTruncated)
}

func PutObj(c *ceph.Ceph) {
	req := ceph.NewPutObjRequest(bucket, objName, filePath)
	resp, err := c.Do(req)
	if err != nil {
		log.Printf("%v\n", err)
		return
	}
	_ = resp
}
