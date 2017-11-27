package ceph

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"sync"
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

func (r *PutObjRequest) Do(p *RequestParam) (Response, error) {
	if p == nil {
		return nil, errors.New("Nil RequestParam")
	}
	if err := p.Validate(); err != nil {
		return nil, fmt.Errorf("Validate RequestParam err, %v", err)
	}

	f, err := os.Open(r.filePath)
	if err != nil {
		return nil, fmt.Errorf("Open %s err, %v", r.filePath, err)
	}
	defer f.Close()

	stat, _ := f.Stat()
	fileSize := stat.Size()
	//fileName := filepath.Base(r.filePath)

	f.Seek(0, os.SEEK_SET)
	md5, err := Base64MD5(f)
	if err != nil {
		return nil, fmt.Errorf("Cal %s md5 err, %v", r.filePath, err)
	}

	// for generate signature
	url := fmt.Sprintf("http://%s/%s/%s", p.Host, r.bucket, r.objName)
	req, err := http.NewRequest("PUT", url, nil)
	if err != nil {
		return nil, fmt.Errorf("New http request err, %v", err)
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
		return nil, fmt.Errorf("Dial %s err, %v", p.Host, err)
	}
	defer conn.Close()

	var (
		notifyErrC = make(chan error, 3)
		respBufC   = make(chan *http.Response, 1)
		writeDoneC = make(chan struct{})
		stopped    bool
		wg         sync.WaitGroup
	)

	defer func() {
		close(respBufC)
		close(writeDoneC)
		close(notifyErrC)
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
			if stopped == true {
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
			time.Sleep(time.Second)
		}
	}(&wg)

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
		//fmt.Printf(buf.String())

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
			//fmt.Printf(string(chunk[:n]))
		}
		if _, err = conn.Write([]byte("\r\n")); err != nil {
			notifyErrC <- fmt.Errorf("Send err, %v", err)
			return
		}
		//fmt.Printf("\r\n")
	}(&wg)

	var (
		retErr  error
		resp    *http.Response
		loop    = true
		timeout = time.NewTimer(100 * 365 * 24 * time.Hour)
	)

	for loop {
		select {
		case retErr = <-notifyErrC:
			loop = false
		case <-writeDoneC:
			timeout.Reset(5 * time.Second)
			// don't stop loop
		case <-timeout.C:
			retErr = errors.New("Read response timeout")
			loop = false
		case resp = <-respBufC:
			fmt.Printf("Response: code: %d, len:%d\n", resp.StatusCode, resp.ContentLength)
			loop = false
		}
	}

	stopped = true
	wg.Wait()

	fmt.Printf("Err: %v\n", retErr)
	/*
		url := fmt.Sprintf("http://%s/%s/%s", p.Host, r.bucket, fileName)
		req, err := http.NewRequest("PUT", url, nil)
		if err != nil {
			return nil, fmt.Errorf("New http request err, %v", err)
		}

		buf := bytes.NewBuffer(nil)
		buf.WriteString("hello")
		req.Body = ioutil.NopCloser(buf)
		req.ContentLength = fileSize

		req.Header.Set("Date", GMTime())
		req.Header.Set("Accept-Encoding", "identity")
		req.Header.Set("Content-Type", "binary/octet-stream")
		req.Header.Set("Content-MD5", md5)
		//req.Header.Set("Content-Length", "5")
		//req.Header.Set("Content-Length", "-1")
		req.Header.Set("Authorization", fmt.Sprintf("%s %s:%s", "AWS", p.AccessKey, Signature(p.SecretKey, req)))

	*/

	/*
		header := bytes.NewBuffer(nil)
		if err = req.Write(header); err != nil {
			return nil, err
		}

		tcpConn, err := net.Dial("tcp", p.Host)
		if err != nil {
			return nil, fmt.Errorf("Connect to ceph(%s) err, %v", p.Host, err)
		}
		defer tcpConn.Close()

		// 向tcp连接写入http头
		if _, err = tcpConn.Write(header.Bytes()); err != nil {
			return nil, fmt.Errorf("Write http header err, %v", err)
		}
		fmt.Printf("%s", header.String())

		f.Seek(0, os.SEEK_SET)
		body := bytes.NewBuffer(nil)
		for {
			chunk := make([]byte, 8192)
			body.Reset()

			n, err := f.Read(chunk)
			if err != nil {
				if err != io.EOF {
					return nil, fmt.Errorf("Read file %s err, %v", r.filePath, err)
				}
				break
			}

			//body.WriteString(fmt.Sprintf("%d\r\n", n))
			body.Write(chunk[:n])
			//body.WriteString("\r\n")
			if _, err = tcpConn.Write(body.Bytes()); err != nil {
				return nil, fmt.Errorf("Send chunk err, %v", err)
			}
			fmt.Printf("%s", body.String())
		}

		//endOfChunk := []byte("0\r\n\r\n")
		//if _, err = tcpConn.Write(endOfChunk); err != nil {
		//	return nil, fmt.Errorf("Send end of chunk err, %v", err)
		//}
		//fmt.Printf("%s", string(endOfChunk))

		resp := make([]byte, 1024)
		n, err := tcpConn.Read(resp)
		if err != nil {
			return nil, fmt.Errorf("Read http response err, %v", err)
		}
		_ = resp
		_ = n
		fmt.Printf("%s\n", string(resp[:n]))
	*/

	/*
		buf := bytes.NewBuffer(nil)
		f.Seek(0, os.SEEK_SET)
		if _, err = io.Copy(buf, f); err != nil {
			return nil, fmt.Errorf("Copy file content to buffer err, %v", err)
		}
		req.Body = ioutil.NopCloser(buf)
		req.ContentLength = int64(buf.Len())
	*/

	/*
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			return nil, fmt.Errorf("Execute request err, %v", err)
		}
		defer resp.Body.Close()

		respBody, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			return nil, fmt.Errorf("Read response body err, %v", err)
		}
		_ = respBody
		fmt.Printf("%s\n", respBody)

		if resp.StatusCode >= 300 {
			return nil, fmt.Errorf("Response StatusCode[%d] >= 300", resp.StatusCode)
		}
	*/

	// TODO:
	return nil, nil
}
