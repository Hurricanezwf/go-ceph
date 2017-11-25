package ceph

import (
	"crypto/hmac"
	"crypto/sha1"
	"encoding/base64"
	"fmt"
	"net/http"
	"sort"
	"strings"
)

var QsaOfInterest map[string]struct{}

func init() {
	// copy from python boto lib(2.34.0) utils.py
	QsaOfInterest = make(map[string]struct{})
	QsaOfInterest["acl"] = struct{}{}
	QsaOfInterest["cors"] = struct{}{}
	QsaOfInterest["defaultObjectAcl"] = struct{}{}
	QsaOfInterest["location"] = struct{}{}
	QsaOfInterest["logging"] = struct{}{}
	QsaOfInterest["partNumber"] = struct{}{}
	QsaOfInterest["policy"] = struct{}{}
	QsaOfInterest["requestPayment"] = struct{}{}
	QsaOfInterest["torrent"] = struct{}{}
	QsaOfInterest["versioning"] = struct{}{}
	QsaOfInterest["versions"] = struct{}{}
	QsaOfInterest["website"] = struct{}{}
	QsaOfInterest["uploads"] = struct{}{}
	QsaOfInterest["uploadId"] = struct{}{}
	QsaOfInterest["response-content-type"] = struct{}{}
	QsaOfInterest["response-content-language"] = struct{}{}
	QsaOfInterest["response-expires"] = struct{}{}
	QsaOfInterest["response-cache-control"] = struct{}{}
	QsaOfInterest["response-content-disposition"] = struct{}{}
	QsaOfInterest["response-content-encoding"] = struct{}{}
	QsaOfInterest["delete"] = struct{}{}
	QsaOfInterest["lifecycle"] = struct{}{}
	QsaOfInterest["tagging"] = struct{}{}
	QsaOfInterest["restore"] = struct{}{}
	QsaOfInterest["storageClass"] = struct{}{}
	QsaOfInterest["websiteConfig"] = struct{}{}
	QsaOfInterest["compose"] = struct{}{}
}

// @param secretKey: 签名的Key
// @param method: HTTP请求的method，取值"PUT" | "POST" | "GET"
// Signature应该在设置完http头后调用(除了Authorization)
func Signature(secretKey string, r *http.Request) string {
	var (
		h          = make(map[string]string)
		sortedKeys = make([]string, 0)
	)

	for k, v := range r.Header {
		lowerKey := strings.ToLower(k)
		switch lowerKey {
		case "date", "content-type", "content-md5":
			h[lowerKey] = strings.Join(v, " ")
			sortedKeys = append(sortedKeys, lowerKey)
		}
	}

	// 补上缺失的计算签名必备的头
	if _, ok := h["date"]; !ok {
		h["date"] = ""
		sortedKeys = append(sortedKeys, "date")
	}
	if _, ok := h["content-md5"]; !ok {
		h["content-md5"] = ""
		sortedKeys = append(sortedKeys, "content-md5")
	}
	if _, ok := h["content-type"]; !ok {
		h["content-type"] = ""
		sortedKeys = append(sortedKeys, "content-type")
	}

	// 所有header按照key排序
	sort.Strings(sortedKeys)

	canonical := r.Method + "\n"
	for _, k := range sortedKeys {
		// 仅添加请求头的值
		canonical += h[k] + "\n"
	}

	// 对uri进行排序，过滤
	var (
		url           = r.URL
		query         = url.Query()
		sortedQuery   = make([]string, 0)
		newQuerySlice = make([]string, 0)
	)

	for k, _ := range query {
		if _, ok := QsaOfInterest[k]; ok {
			sortedQuery = append(sortedQuery, k)
		}
	}
	sort.Strings(sortedQuery)

	for _, k := range sortedQuery {
		queryVal := query[k]
		if len(queryVal) <= 0 {
			// 对于xxx?acl这种情况，直接添加
			newQuerySlice = append(newQuerySlice, k)
			continue
		}
		for _, v := range queryVal {
			newQuerySlice = append(newQuerySlice, fmt.Sprintf("%s=%s", k, v))
		}
	}

	canonical += url.Path
	if len(newQuerySlice) > 0 {
		canonical += "?" + strings.Join(newQuerySlice, "&")
	}

	hashmac := Hashmac([]byte(canonical), []byte(secretKey))
	b64 := base64.StdEncoding.EncodeToString(hashmac)
	return b64
}

func Hashmac(msg, key []byte) []byte {
	mac := hmac.New(sha1.New, key)
	mac.Write(msg)
	return mac.Sum(nil)
}
