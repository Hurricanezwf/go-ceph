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
	"os"
	"sync"
	"sync/atomic"
	"time"
)

type PutObjRequest struct {
	bucket   string // [required]
	objName  string // [required]
	filePath string // [required]
}

func NewPutObjRequest(bucket, objName, filePath string) *PutObjRequest {
	return &PutObjRequest{
		bucket:   bucket,
		objName:  objName,
		filePath: filePath,
	}
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

	// 计算文件大小和md5
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

	// 读取返回, 因为对方发送完回复后会立刻关闭连接，所以必须在对方发送的时候保证有读操作
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

		// 写http头
		if _, err = conn.Write(buf.Bytes()); err != nil {
			notifyErrC <- fmt.Errorf("Write http header err, %v", err)
			return
		}

		// 写body内容
		f.Seek(0, os.SEEK_SET)
		chunk := make([]byte, 8192)
		for {
			n, err := f.Read(chunk)
			if err != nil {
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
			if _, err = conn.Write(chunk[:n]); err != nil {
				notifyErrC <- fmt.Errorf("Send err, %v", err)
				return
			}
		}
		if _, err = conn.Write([]byte("\r\n")); err != nil {
			notifyErrC <- fmt.Errorf("Send err, %v", err)
			return
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

			poresp.ETag = resp.Header.Get("ETag")
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
	ETag string

	err error
}

func (r PutObjResponse) Err() error {
	return r.err
}
