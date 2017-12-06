package ceph

import (
	"bytes"
	"encoding/xml"
	"errors"
	"fmt"
	"io/ioutil"
	"net"
	"net/http"
)

var (
	ErrBucketNotExist = errors.New("Bucket not exist")
)

////////////////////////////////////////////////////////////
type RequestParam struct {
	Host      string //ip:port
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

/////////////////////////////////////////////////////////////
type Owner struct {
	XMLName     xml.Name `xml:"Owner"`
	ID          string   `xml:"ID"`
	DisplayName string   `xml:"DisplayName"`
}

type Buckets struct {
	XMLName    xml.Name `xml:"Buckets"`
	BucketList []Bucket `xml:"Bucket"`
}

type Bucket struct {
	XMLName      xml.Name `xml:"Bucket"`
	Name         string   `xml:"Name"`
	CreationDate string   `xml:"CreationDate"`
}

/////////////////////////////////////////////////////////////
type GetAllBucketsRequest struct {
}

func NewGetAllBucketsRequest() *GetAllBucketsRequest {
	return &GetAllBucketsRequest{}
}

func (r *GetAllBucketsRequest) Do(p *RequestParam) Response {
	var gabresp = &GetAllBucketsResponse{}

	// 参数校验
	if p == nil {
		gabresp.err = errors.New("Nil RequestParam")
		return gabresp
	}
	if err := p.Validate(); err != nil {
		gabresp.err = fmt.Errorf("Validate RequestParam err, %v", err)
		return gabresp
	}

	// 发送请求
	url := fmt.Sprintf("http://%s/", p.Host)
	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		gabresp.err = fmt.Errorf("New http request err, %v", err)
		return gabresp
	}

	req.Header.Set("Date", GMTime())
	req.Header.Set("Accept-Encoding", "identity")
	req.Header.Set("Authorization", fmt.Sprintf("%s %s:%s", "AWS", p.AccessKey, Signature(p.SecretKey, req)))

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		gabresp.err = fmt.Errorf("Do request err, %v", err)
		return gabresp
	}
	defer resp.Body.Close()

	// 解析响应
	respBody, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		gabresp.err = fmt.Errorf("Read response body err, %v", err)
		return gabresp
	}

	if resp.StatusCode != 200 {
		gabresp.err = errors.New(string(respBody))
		return gabresp
	}

	if err = xml.Unmarshal(respBody, gabresp); err != nil {
		gabresp.err = fmt.Errorf("Unmarshal response body err, %v", err)
		return gabresp
	}

	return gabresp
}

type GetAllBucketsResponse struct {
	XMLName xml.Name `xml:"ListAllMyBucketsResult"`
	Owner   Owner    `xml:"Owner"`
	Buckets Buckets  `xml:"Buckets"`

	err error
}

func (r GetAllBucketsResponse) Err() error {
	return r.err
}

////////////////////////////////////////////////////////////////////////
type GetBucketOption struct {
	Prefix    string
	Delimiter string
	Marker    string
	Maxkeys   uint32
}

func DefaultGetBucketOption() *GetBucketOption {
	return &GetBucketOption{
		Maxkeys: 1000,
	}
}

func (p GetBucketOption) UrlStr() string {
	buf := bytes.NewBuffer(nil)
	buf.WriteString(fmt.Sprintf("max-keys=%d", p.Maxkeys))
	if len(p.Prefix) > 0 {
		buf.WriteString(fmt.Sprintf("&prefix=%s", p.Prefix))
	}
	if len(p.Delimiter) > 0 {
		buf.WriteString(fmt.Sprintf("&delimiter=%s", p.Delimiter))
	}
	if len(p.Marker) > 0 {
		buf.WriteString(fmt.Sprintf("&marker=%s", p.Marker))
	}
	return buf.String()
}

type GetBucketRequest struct {
	bucket string           // [required]
	opt    *GetBucketOption // [optional]

	// validate check if bucket is existed before getting, default is false
	validate bool
}

func NewGetBucketRequest(bucket string) *GetBucketRequest {
	r := &GetBucketRequest{
		bucket:   bucket,
		opt:      nil,
		validate: false,
	}
	return r
}

func (r *GetBucketRequest) SetOption(opt *GetBucketOption) {
	r.opt = opt
}

func (r *GetBucketRequest) SetValidate(v bool) {
	r.validate = v
}

func (r *GetBucketRequest) Do(p *RequestParam) Response {
	var gbresp = &GetBucketResponse{}

	// 参数校验
	if p == nil {
		gbresp.err = errors.New("Nil RequestParam")
		return gbresp
	}
	if err := p.Validate(); err != nil {
		gbresp.err = fmt.Errorf("Validate RequestParam err, %v", err)
		return gbresp
	}

	// 验证bucket是否存在
	if r.validate {
		result := NewHeadBucketRequest(r.bucket).Do(p)
		if err := result.Err(); err != nil {
			gbresp.err = fmt.Errorf("Validate bucket(%s) err, %v", r.bucket, err)
			return gbresp
		}
		if result.(*HeadBucketResponse).IsExisted == false {
			gbresp.err = ErrBucketNotExist
			return gbresp
		}
	}

	if r.opt == nil {
		r.opt = DefaultGetBucketOption()
	}

	// 请求获取
	url := fmt.Sprintf("http://%s/%s?%s", p.Host, r.bucket, r.opt.UrlStr())
	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		gbresp.err = fmt.Errorf("New http request err, %v", err)
		return gbresp
	}

	req.Header.Set("Date", GMTime())
	req.Header.Set("Accept-Encoding", "identity")
	req.Header.Set("Authorization", fmt.Sprintf("%s %s:%s", "AWS", p.AccessKey, Signature(p.SecretKey, req)))

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		gbresp.err = fmt.Errorf("Do request err, %v", err)
		return gbresp
	}
	defer resp.Body.Close()

	// 解析响应
	respBody, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		gbresp.err = fmt.Errorf("Read response body err, %v", err)
		return gbresp
	}
	//fmt.Printf("Response: %s\n", string(respBody))

	if resp.StatusCode != 200 {
		gbresp.err = errors.New(string(respBody))
		return gbresp
	}

	if err = xml.Unmarshal(respBody, gbresp); err != nil {
		gbresp.err = fmt.Errorf("Unmarshal response body err, %v", err)
		return gbresp
	}
	return gbresp
}

type GetBucketResponse struct {
	XMLName     xml.Name `xml:"ListBucketResult"`
	Name        string   `xml:"Name"`
	Prefix      string   `xml:"Prefix"`
	Marker      string   `xml:"Marker"`
	MaxKeys     uint32   `xml:"MaxKeys"`
	IsTruncated bool     `xml:"IsTruncated"`

	err error
}

func (r GetBucketResponse) Err() error {
	return r.err
}

/////////////////////////////////////////////////////////////////
type HeadBucketRequest struct {
	bucket string // [required]
}

func NewHeadBucketRequest(bucket string) *HeadBucketRequest {
	return &HeadBucketRequest{
		bucket: bucket,
	}
}

func (r *HeadBucketRequest) Do(p *RequestParam) Response {
	var hbresp = &HeadBucketResponse{}

	// 参数校验
	if p == nil {
		hbresp.err = errors.New("Nil RequestParam")
		return hbresp
	}
	if err := p.Validate(); err != nil {
		hbresp.err = fmt.Errorf("Validate RequestParam err, %v", err)
		return hbresp
	}

	// 发送请求
	url := fmt.Sprintf("http://%s/%s/", p.Host, r.bucket)
	req, err := http.NewRequest("HEAD", url, nil)
	if err != nil {
		hbresp.err = fmt.Errorf("New http request err, %v", err)
		return hbresp
	}

	req.Header.Set("Date", GMTime())
	req.Header.Set("Accept-Encoding", "identity")
	req.Header.Set("Authorization", fmt.Sprintf("%s %s:%s", "AWS", p.AccessKey, Signature(p.SecretKey, req)))

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		hbresp.err = fmt.Errorf("Do request err, %v", err)
		return hbresp
	}
	defer resp.Body.Close()

	hbresp.IsExisted = (resp.StatusCode == 200)
	return hbresp
}

type HeadBucketResponse struct {
	IsExisted bool

	err error
}

func (r HeadBucketResponse) Err() error {
	return r.err
}
