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

type Request interface {
	Do(p *RequestParam) (Response, error)
}

type Response interface {
	Detail() []byte
}

////////////////////////////////////////////////////////////
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

func (r *GetAllBucketsRequest) Do(p *RequestParam) (Response, error) {
	if p == nil {
		return nil, errors.New("Nil RequestParam")
	}
	if err := p.Validate(); err != nil {
		return nil, fmt.Errorf("Validate RequestParam err, %v", err)
	}

	url := fmt.Sprintf("http://%s/", p.Host)
	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		return nil, fmt.Errorf("New http request err, %v", err)
	}

	req.Header.Set("Date", GMTime())
	req.Header.Set("Accept-Encoding", "identity")
	req.Header.Set("Authorization", fmt.Sprintf("%s %s:%s", "AWS", p.AccessKey, Signature(p.SecretKey, req)))

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		return nil, fmt.Errorf("Response StatusCode[%d] != 200", resp.StatusCode)
	}

	respBody, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("Read response body err, %v", err)
	}
	//fmt.Printf("Response: %s\n", string(respBody))

	gabresp := &GetAllBucketsResponse{}
	if err = xml.Unmarshal(respBody, gabresp); err != nil {
		return nil, fmt.Errorf("Unmarshal response body err, %v", err)
	}
	gabresp.respBody = respBody

	return gabresp, nil
}

type GetAllBucketsResponse struct {
	XMLName xml.Name `xml:"ListAllMyBucketsResult"`
	Owner   Owner    `xml:"Owner"`
	Buckets Buckets  `xml:"Buckets"`

	respBody []byte
}

func (r GetAllBucketsResponse) Detail() []byte {
	return r.respBody
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

// @param bucket  : bucket name
// @param opt     : bucket option for quering, if it's nil, default will be used
// @param validte : whether validating bucket is existed or not
func NewGetBucketRequest(bucket string, opt *GetBucketOption, validate bool) *GetBucketRequest {
	r := &GetBucketRequest{
		bucket:   bucket,
		opt:      opt,
		validate: validate,
	}
	if r.opt == nil {
		r.opt = DefaultGetBucketOption()
	}
	return r
}

func (r *GetBucketRequest) SetOption(opt *GetBucketOption) {
	r.opt = opt
}

func (r *GetBucketRequest) Do(p *RequestParam) (Response, error) {
	if p == nil {
		return nil, errors.New("Nil RequestParam")
	}
	if err := p.Validate(); err != nil {
		return nil, fmt.Errorf("Validate RequestParam err, %v", err)
	}

	if r.validate {
		exist, err := BucketExisted(r.bucket, p)
		if err != nil {
			return nil, fmt.Errorf("Validate bucket(%s) err, %v", r.bucket, err)
		}
		if !exist {
			return nil, ErrBucketNotExist
		}
	}

	url := fmt.Sprintf("http://%s/%s?%s", p.Host, r.bucket, r.opt.UrlStr())
	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		return nil, fmt.Errorf("New http request err, %v", err)
	}

	req.Header.Set("Date", GMTime())
	req.Header.Set("Accept-Encoding", "identity")
	req.Header.Set("Authorization", fmt.Sprintf("%s %s:%s", "AWS", p.AccessKey, Signature(p.SecretKey, req)))

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		return nil, fmt.Errorf("Response StatusCode[%d] != 200", resp.StatusCode)
	}

	respBody, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("Read response body err, %v", err)
	}
	//fmt.Printf("Response: %s\n", string(respBody))

	gbresp := &GetBucketResponse{}
	if err = xml.Unmarshal(respBody, gbresp); err != nil {
		return nil, fmt.Errorf("Unmarshal response body err, %v", err)
	}
	gbresp.respBody = respBody

	return gbresp, nil
}

type GetBucketResponse struct {
	XMLName     xml.Name `xml:"ListBucketResult"`
	Name        string   `xml:"Name"`
	Prefix      string   `xml:"Prefix"`
	Marker      string   `xml:"Marker"`
	MaxKeys     uint32   `xml:"MaxKeys"`
	IsTruncated bool     `xml:"IsTruncated"`

	respBody []byte
}

func (r GetBucketResponse) Detail() []byte {
	return r.respBody
}

func BucketExisted(bucket string, p *RequestParam) (bool, error) {
	url := fmt.Sprintf("http://%s/%s/", p.Host, bucket)
	req, err := http.NewRequest("HEAD", url, nil)
	if err != nil {
		return false, fmt.Errorf("New http request err, %v", err)
	}

	req.Header.Set("Date", GMTime())
	req.Header.Set("Accept-Encoding", "identity")
	req.Header.Set("Authorization", fmt.Sprintf("%s %s:%s", "AWS", p.AccessKey, Signature(p.SecretKey, req)))

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return false, err
	}
	defer resp.Body.Close()

	return resp.StatusCode == 200, nil
}
