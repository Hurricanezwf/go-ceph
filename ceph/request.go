package ceph

import (
	"errors"
	"fmt"
	"io/ioutil"
	"net"
	"net/http"
	"time"
)

type Request interface {
	Do(p *RequestParam) (Response, error)
}

type Response interface {
}

type RequestParam struct {
	Host      string
	AccessKey string
	SecretKey string
}

func (p RequestParam) Validate() error {
	if _, err := net.ResolveTCPAddr("tcp", p.Host); err != nil {
		return errors.New("Invalid host")
	}

	if len(p.AccessKey) <= 0 {
		return errors.New("Empty AccessKey")
	}

	if len(p.SecretKey) <= 0 {
		return errors.New("Empty SecretKey")
	}

	return nil
}

/////////////////////////////////////////////////////
type GetBucketACLRequest struct {
	// 要获取的指定的bucket，如果为空，表示列出所有
	bucket string
}

type GetBucketACLResponse struct {
}

func NewGetBucketACLRequest(bucket string) *GetBucketACLRequest {
	return &GetBucketACLRequest{
		bucket: bucket,
	}
}

func (r *GetBucketACLRequest) Do(p *RequestParam) (Response, error) {
	if p == nil {
		return nil, errors.New("Nil RequestParam")
	}

	if err := p.Validate(); err != nil {
		return nil, fmt.Errorf("Validate RequestParam err, %v", err)
	}

	var url string
	if len(r.bucket) <= 0 {
		url = fmt.Sprintf("http://%s/", p.Host)
	} else {
		url = fmt.Sprintf("http://%s/%s?acl", p.Host, r.bucket)
	}

	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		return nil, fmt.Errorf("New http request err, %v", err)
	}

	req.Header.Set("Date", time.Now().UTC().String())
	req.Header.Set("Authorization", fmt.Sprintf("%s %s:%s", "AWS", p.AccessKey, Signature(p.SecretKey, req)))

	c := http.Client{}
	resp, err := c.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		return nil, fmt.Errorf("Response StatusCode[%d] != 200", resp.StatusCode)
	}

	content, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("Read response body err, %v", err)
	}
	fmt.Printf("Response: %s\n", string(content))

	return nil, nil
}
