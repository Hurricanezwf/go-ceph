package ceph

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"net"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

const (
	TypeByURL  = 1
	TypeByName = 2
)

type PutObjRequest struct {
	bucket   string // [required]
	objName  string // [required]
	filePath string // [required]

	// 是否生成下载url,如果为true，则生成带认证的下载链接返回
	// 默认不生成
	genUrl  bool
	signed  bool
	expired int64

	/* 以下内部使用 */
	enableProgress bool
	progress       atomic.Value // [0,100] float64
}

func NewPutObjRequest(bucket, objName, filePath string) *PutObjRequest {
	return &PutObjRequest{
		bucket:   bucket,
		objName:  objName,
		filePath: filePath,
	}
}

// @param signed  : 是否在URL中生成签名
// @param expired : 下载链接有效时间，单位秒
func (r *PutObjRequest) EnableGenDownloadUrl(signed bool, expired int64) *PutObjRequest {
	r.genUrl = true
	r.signed = signed
	r.expired = expired
	return r
}

func (r *PutObjRequest) DisableGenDownloadUrl() *PutObjRequest {
	r.genUrl = false
	r.signed = false
	r.expired = 0
	return r
}

func (r *PutObjRequest) SetEnableProgress(enable bool) *PutObjRequest {
	r.enableProgress = enable
	r.progress.Store(float64(0))
	return r
}

func (r *PutObjRequest) Progress() float64 {
	v := r.progress.Load()
	return v.(float64)
}

func (r *PutObjRequest) Do(p *RequestParam) Response {
	var poresp = &PutObjResponse{}

	// 参数校验
	if p == nil {
		poresp.err = errors.New("Nil RequestParam")
		return poresp
	}
	if err := p.Validate(); err != nil {
		poresp.err = fmt.Errorf("Validate RequestParam err, %v", err)
		return poresp
	}

	// 计算文件大小和base64(md5)
	f, err := os.Open(r.filePath)
	if err != nil {
		poresp.err = fmt.Errorf("Open %s err, %v", r.filePath, err)
		return poresp
	}
	defer f.Close()

	stat, _ := f.Stat()
	fileSize := stat.Size()

	md5, err := Base64MD5(f)
	if err != nil {
		poresp.err = fmt.Errorf("Cal %s md5 err, %v", r.filePath, err)
		return poresp
	}

	// 发送请求
	// 新建一个http.Request是为了生成签名用
	url := fmt.Sprintf("http://%s/%s/%s", p.Host, r.bucket, r.objName)
	req, err := http.NewRequest("PUT", url, nil)
	if err != nil {
		poresp.err = fmt.Errorf("New http request err, %v", err)
		return poresp
	}
	req.Header.Set("Date", GMTime())
	req.Header.Set("Content-Type", "binary/octet-stream")
	req.Header.Set("Content-MD5", md5)
	sign := Signature(p.SecretKey, req)

	buf := bytes.NewBuffer(nil)
	buf.WriteString(fmt.Sprintf("PUT /%s/%s HTTP/1.1\r\n", r.bucket, r.objName))
	buf.WriteString(fmt.Sprintf("Host: %s\r\n", p.Host))
	buf.WriteString("User-Agent: Go-http-client/1.1\r\n")
	buf.WriteString("Accept-Encoding: identity\r\n")
	buf.WriteString(fmt.Sprintf("Content-Type: %s\r\n", req.Header.Get("Content-Type")))
	buf.WriteString(fmt.Sprintf("Content-Length: %d\r\n", fileSize))
	buf.WriteString(fmt.Sprintf("Content-MD5: %s\r\n", req.Header.Get("Content-MD5")))
	buf.WriteString(fmt.Sprintf("Date: %s\r\n", req.Header.Get("Date")))
	buf.WriteString(fmt.Sprintf("Authorization: AWS %s:%s\r\n", p.AccessKey, sign))
	buf.WriteString("\r\n")

	// 新建到ceph的tcp连接
	conn, err := net.DialTimeout("tcp", p.Host, 5*time.Second)
	if err != nil {
		poresp.err = fmt.Errorf("Dial %s err, %v", p.Host, err)
		return poresp
	}
	defer conn.Close()

	var (
		notifyErrC = make(chan error, 2)
		respBufC   = make(chan *http.Response, 1)
		writeDoneC = make(chan struct{})
		stopped    = int32(0)
		wg         sync.WaitGroup
	)

	defer func() {
		close(notifyErrC)
		close(respBufC)
		close(writeDoneC)
	}()

	// 读取返回, 因为对方发送完回复后会立刻关闭连接，所以必需在对方发送的时候保证有读操作
	go func(w *sync.WaitGroup) {
		w.Add(1)
		defer func() {
			recover()
			w.Done()
		}()

		respReader := bufio.NewReader(conn)
		for {
			if atomic.LoadInt32(&stopped) > int32(0) {
				break
			}
			conn.SetReadDeadline(time.Now().Add(time.Second))
			resp, err := http.ReadResponse(respReader, nil)
			if err != nil {
				opErr, ok := err.(*net.OpError)
				if !ok || opErr.Timeout() == false {
					notifyErrC <- err
					return
				}
			} else {
				respBufC <- resp
			}
			time.Sleep(100 * time.Millisecond)
		}
	}(&wg)

	// 发送请求
	go func(w *sync.WaitGroup) {
		w.Add(1)
		defer func() {
			recover()
			w.Done()
		}()

		var (
			err            error
			n              int
			writeCount     int64
			lastUpdateTime time.Time
		)

		// 写http头
		if _, err = conn.Write(buf.Bytes()); err != nil {
			notifyErrC <- fmt.Errorf("Write http header err, %v", err)
			return
		}

		// 写body内容
		f.Seek(0, os.SEEK_SET)
		chunk := make([]byte, 8192)
		for {
			if n, err = f.Read(chunk); err != nil {
				if err == io.EOF {
					break
				}
				notifyErrC <- fmt.Errorf("Read %s err, %v", r.filePath, err)
				return
			}
			if n <= 0 {
				break
			}

			conn.SetWriteDeadline(time.Now().Add(5 * time.Second))
			if n, err = conn.Write(chunk[:n]); err != nil {
				notifyErrC <- fmt.Errorf("Send err, %v", err)
				return
			}
			writeCount += int64(n)

			// 记录进度
			if r.enableProgress {
				now := time.Now()
				if now.Sub(lastUpdateTime) < time.Second {
					continue
				}
				if fileSize > 0 {
					r.progress.Store(float64(writeCount * int64(100) / fileSize))
					lastUpdateTime = now
				}
			}
		}
		if _, err = conn.Write([]byte("\r\n")); err != nil {
			notifyErrC <- fmt.Errorf("Send err, %v", err)
			return
		}
		if r.enableProgress {
			r.progress.Store(float64(100))
		}
	}(&wg)

	// 解析返回
	var (
		resp    *http.Response
		loop    = true
		timeout = time.NewTimer(100 * 365 * 24 * time.Hour)
	)

	for loop {
		select {
		case poresp.err = <-notifyErrC:
			loop = false
		case <-writeDoneC:
			timeout.Reset(5 * time.Second)
			// don't stop loop
		case <-timeout.C:
			poresp.err = errors.New("Read response timeout")
			loop = false
		case resp = <-respBufC:
			if resp.StatusCode < 200 {
				// continue receive
				break
			}

			loop = false

			respBody, err := ioutil.ReadAll(resp.Body)
			if err != nil {
				poresp.err = fmt.Errorf("Read response body err, %v", err)
				break
			}

			if resp.StatusCode != 200 {
				poresp.err = errors.New(string(respBody))
				break
			}

			if r.genUrl {
				download, err := GenDownloadUrl(r.bucket, r.objName, p, r.signed, r.expired)
				if err != nil {
					poresp.err = errors.New("Generate download url failed")
					break
				}
				poresp.DownloadUrl = download
			}

			poresp.ETag = strings.Trim(resp.Header.Get("ETag"), "\"")
			poresp.Base64Md5 = md5
		}

		if resp != nil {
			resp.Body.Close()
		}
	}

	atomic.AddInt32(&stopped, int32(1))
	wg.Wait()

	return poresp
}

type PutObjResponse struct {
	ETag        string
	Base64Md5   string
	DownloadUrl string

	err error
}

func (r PutObjResponse) Err() error {
	return r.err
}

//////////////////////////////////////////////////////////////////
type GetObjRequest struct {
	tp int

	// 当TypeByURL时必需
	url string

	// 当tp==TypeByName时必需
	bucket  string
	objName string

	// 下载的文件保存的位置
	savePath string

	// 可选参数, 如果长度大于0，则下载完成后进行验证
	// base64(md5)的值
	base64Md5 string

	// 对象大小
	objSize int64

	// 下载进度
	enableProgress bool
	progress       atomic.Value // float64 [0,100]
}

func NewGetObjRequest(bucket, objName, savePath string) *GetObjRequest {
	return &GetObjRequest{
		tp:       TypeByName,
		bucket:   bucket,
		objName:  objName,
		savePath: savePath,
	}
}

func NewGetObjByUrlRequest(url, savePath string) *GetObjRequest {
	return &GetObjRequest{
		tp:       TypeByURL,
		url:      url,
		savePath: savePath,
	}
}

func (r *GetObjRequest) SetBase64Md5(v string) *GetObjRequest {
	r.base64Md5 = v
	return r
}

func (r *GetObjRequest) SetEnableProgress(enable bool) *GetObjRequest {
	r.enableProgress = enable
	r.progress.Store(float64(0))
	return r
}

func (r *GetObjRequest) ObjSize() int64 {
	return r.objSize
}

func (r *GetObjRequest) Progress() float64 {
	v := r.progress.Load()
	return v.(float64)
}

func (r *GetObjRequest) Do(p *RequestParam) Response {
	var goresp = &GetObjResponse{}

	// 参数校验
	if p == nil {
		goresp.err = errors.New("Nil RequestParam")
		return goresp
	}
	if err := p.Validate(); err != nil {
		goresp.err = fmt.Errorf("Validate RequestParam err, %v", err)
		return goresp
	}

	switch r.tp {
	case TypeByURL:
		return r.getByURL(p)
	case TypeByName:
		return r.getByName(p)
	default:
		goresp.err = fmt.Errorf("Unknown get obj type %d", r.tp)
		return goresp
	}

	return goresp
}

func (r *GetObjRequest) getByURL(p *RequestParam) Response {
	var goresp = &GetObjResponse{}

	// 获取对象信息
	getInfoReq := NewGetObjInfoByUrlRequest(r.url)
	getInfoResp := getInfoReq.Do(p)
	if err := getInfoResp.Err(); err != nil {
		goresp.err = fmt.Errorf("Get object info err, %v", err)
		return goresp
	}
	r.objSize = getInfoResp.(*GetObjInfoResponse).Size

	// 发送获取对象请求
	req, err := http.NewRequest("GET", r.url, nil)
	if err != nil {
		goresp.err = fmt.Errorf("New http request err, %v", err)
		return goresp
	}

	req.Header.Set("Date", GMTime())
	req.Header.Set("Accept-Encoding", "identity")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		goresp.err = fmt.Errorf("Do request err, %v", err)
		return goresp
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		body, _ := ioutil.ReadAll(resp.Body)
		goresp.err = errors.New(string(body))
		return goresp
	}

	// 保存对象文件到本地
	if err = r.save(r.savePath, resp.Body); err != nil {
		goresp.err = err
		return goresp
	}

	return goresp
}

func (r *GetObjRequest) getByName(p *RequestParam) Response {
	var goresp = &GetObjResponse{}

	// 获取对象信息
	getInfoReq := NewGetObjInfoRequest(r.bucket, r.objName)
	getInfoResp := getInfoReq.Do(p)
	if err := getInfoResp.Err(); err != nil {
		goresp.err = fmt.Errorf("Get object info err, %v", err)
		return goresp
	}
	r.objSize = getInfoResp.(*GetObjInfoResponse).Size

	// 发送获取对象请求
	url := fmt.Sprintf("http://%s/%s/%s", p.Host, r.bucket, r.objName)
	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		goresp.err = fmt.Errorf("New http request err, %v", err)
		return goresp
	}

	req.Header.Set("Date", GMTime())
	req.Header.Set("Accept-Encoding", "identity")
	req.Header.Set("Authorization", fmt.Sprintf("%s %s:%s", "AWS", p.AccessKey, Signature(p.SecretKey, req)))

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		goresp.err = fmt.Errorf("Do request err, %v", err)
		return goresp
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		body, _ := ioutil.ReadAll(resp.Body)
		goresp.err = errors.New(string(body))
		return goresp
	}

	// 保存对象文件到本地
	if err = r.save(r.savePath, resp.Body); err != nil {
		goresp.err = err
		return goresp
	}

	return goresp
}

func (r *GetObjRequest) save(savePath string, src io.Reader) error {
	savePath, _ = filepath.Abs(savePath)
	tmpPath := r.savePath + ".download"

	saveErr := func() error {
		f, err := os.OpenFile(tmpPath, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0644)
		if err != nil {
			return fmt.Errorf("Open file %s err, %v", tmpPath, err)
		}
		defer f.Close()

		// 启用进度显示
		var stopC chan struct{}
		defer func() {
			if stopC != nil {
				close(stopC)
			}
		}()
		if r.enableProgress {
			stopC = make(chan struct{})
			go func() {
				for {
					select {
					case <-stopC:
						r.progress.Store(float64(100))
						return
					case <-time.After(time.Second):
						info, err := f.Stat()
						if err == nil {
							continue
						}
						if r.objSize > 0 {
							r.progress.Store(float64((info.Size() - 1) * int64(100) / r.objSize))
						}
					}
				}
			}()
		}

		written, err := io.Copy(f, src)
		if err != nil {
			return fmt.Errorf("Write file content err, %v", err)
		}

		if written != r.objSize {
			return fmt.Errorf("Loss of data during writing, %d bytes written, %d is needed", written, r.objSize)
		}

		if err = f.Sync(); err != nil {
			return fmt.Errorf("Sync object to file err, %v", err)
		}

		if len(r.base64Md5) > 0 {
			b64Md5, err := Base64MD5(f)
			if err != nil {
				return fmt.Errorf("Cal Base64MD5 err, %v", err)
			}
			if b64Md5 != r.base64Md5 {
				return errors.New("Base64Md5 not equal")
			}
		}

		if err = os.Rename(tmpPath, r.savePath); err != nil {
			return fmt.Errorf("Rename file err, %v", err)
		}
		return nil
	}()

	if saveErr != nil {
		os.Remove(tmpPath)
	}
	return saveErr
}

type GetObjResponse struct {
	err error
}

func (r GetObjResponse) Err() error {
	return r.err
}

//////////////////////////////////////////////////////////////
type GetObjInfoRequest struct {
	tp int

	// 当tp==TypeByURL时必需
	url string

	// 当tp==TypeByName时必需
	bucket  string
	objName string
}

func NewGetObjInfoRequest(bucket, objName string) *GetObjInfoRequest {
	return &GetObjInfoRequest{
		tp:      TypeByName,
		bucket:  bucket,
		objName: objName,
	}
}

func NewGetObjInfoByUrlRequest(url string) *GetObjInfoRequest {
	return &GetObjInfoRequest{
		tp:  TypeByURL,
		url: url,
	}
}

func (r *GetObjInfoRequest) Do(p *RequestParam) Response {
	var goiresp = &GetObjInfoResponse{}

	// 参数校验
	if p == nil {
		goiresp.err = errors.New("Nil RequestParam")
		return goiresp
	}
	if err := p.Validate(); err != nil {
		goiresp.err = fmt.Errorf("Validate RequestParam err, %v", err)
		return goiresp
	}

	switch r.tp {
	case TypeByName:
		return r.getByName(p)
	case TypeByURL:
		return r.getByURL(p)
	default:
		goiresp.err = fmt.Errorf("Unknown get object info type %d", r.tp)
		return goiresp
	}

	return goiresp
}

func (r *GetObjInfoRequest) getByURL(p *RequestParam) Response {
	var goiresp = &GetObjInfoResponse{}

	req, err := http.NewRequest("HEAD", r.url, nil)
	if err != nil {
		goiresp.err = fmt.Errorf("New http request err, %v", err)
		return goiresp
	}

	req.Header.Set("Date", GMTime())
	req.Header.Set("Accept-Encoding", "identity")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		goiresp.err = fmt.Errorf("Do request err, %v", err)
		return goiresp
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		goiresp.err = fmt.Errorf("Response StatusCode(%d) != 200", resp.StatusCode)
		return goiresp
	}

	l, _ := strconv.ParseInt(resp.Header.Get("Content-Length"), 10, 64)

	goiresp.Size = l
	goiresp.LastModified = resp.Header.Get("Last-Modified")
	goiresp.ETag = resp.Header.Get("ETag")

	return goiresp
}

func (r *GetObjInfoRequest) getByName(p *RequestParam) Response {
	var goiresp = &GetObjInfoResponse{}

	url := fmt.Sprintf("http://%s/%s/%s", p.Host, r.bucket, r.objName)
	req, err := http.NewRequest("HEAD", url, nil)
	if err != nil {
		goiresp.err = fmt.Errorf("New http request err, %v", err)
		return goiresp
	}

	req.Header.Set("Date", GMTime())
	req.Header.Set("Accept-Encoding", "identity")
	req.Header.Set("Authorization", fmt.Sprintf("%s %s:%s", "AWS", p.AccessKey, Signature(p.SecretKey, req)))

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		goiresp.err = fmt.Errorf("Do request err, %v", err)
		return goiresp
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		goiresp.err = fmt.Errorf("Response StatusCode(%d) != 200", resp.StatusCode)
		return goiresp
	}

	l, _ := strconv.ParseInt(resp.Header.Get("Content-Length"), 10, 64)

	goiresp.Size = l
	goiresp.LastModified = resp.Header.Get("Last-Modified")
	goiresp.ETag = resp.Header.Get("ETag")

	return goiresp
}

type GetObjInfoResponse struct {
	Size         int64
	LastModified string
	ETag         string

	err error
}

func (r GetObjInfoResponse) Err() error {
	return r.err
}

//////////////////////////////////////////////////////////////////
// GenDownloadUrl: 生成对象的下载链接
// @param signed  : 是否携带签名
// @param expired : 下载链接有效时间
func GenDownloadUrl(bucket, objName string, p *RequestParam, signed bool, expired int64) (string, error) {
	// 检测object
	req := NewGetObjInfoRequest(bucket, objName)
	resp := req.Do(p)
	if err := resp.Err(); err != nil {
		return "", errors.New("Bad object")
	}

	bucket = url.PathEscape(bucket)
	objName = url.PathEscape(objName)

	// 下面生成签名需要依赖这个path
	path := fmt.Sprintf("http://%s/%s/%s", p.Host, bucket, objName)

	if signed {
		expiredStr := fmt.Sprintf("%d", time.Now().Add(time.Duration(expired)*time.Second).Unix())
		req, _ := http.NewRequest("GET", path, nil)
		req.Header.Set("Expires", expiredStr)

		signature := url.QueryEscape(Signature(p.SecretKey, req))
		expiredStr = url.QueryEscape(expiredStr)

		path = fmt.Sprintf("http://%s/%s/%s?Signature=%s&Expires=%s&AWSAccessKeyId=%s",
			p.Host, bucket, objName, signature, expiredStr, p.AccessKey)
	}

	return path, nil
}
