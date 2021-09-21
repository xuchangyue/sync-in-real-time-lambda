package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"time"
	"net"
	"net/http"
	"io"
	"bytes"
	


	"github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-lambda-go/lambda"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	//"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"golang.org/x/net/http2"
	"github.com/aws/aws-xray-sdk-go/xray"

)

type Config struct {
	AWSAccessKey     string
	AWSSecretKey string
	S3Region        string
	S3Bucket        string
	FileURL			string
}

var (
	config Config
	sess *session.Session
	svc *s3.S3
)

type HTTPClientSettings struct {
    Connect          time.Duration
    ConnKeepAlive    time.Duration
    ExpectContinue   time.Duration
    IdleConn         time.Duration
    MaxAllIdleConns  int
    MaxHostIdleConns int
    ResponseHeader   time.Duration
    TLSHandshake     time.Duration
}

func NewHTTPClientWithSettings(httpSettings HTTPClientSettings) *http.Client {
    tr := &http.Transport{
        ResponseHeaderTimeout: httpSettings.ResponseHeader,
        Proxy: http.ProxyFromEnvironment,
        DialContext: (&net.Dialer{
            KeepAlive: httpSettings.ConnKeepAlive,
            DualStack: true,
            Timeout:   httpSettings.Connect,
        }).DialContext,
        MaxIdleConns:          httpSettings.MaxAllIdleConns,
        IdleConnTimeout:       httpSettings.IdleConn,
        TLSHandshakeTimeout:   httpSettings.TLSHandshake,
        MaxIdleConnsPerHost:   httpSettings.MaxHostIdleConns,
        ExpectContinueTimeout: httpSettings.ExpectContinue,
    }

    // So client makes HTTP/2 requests
    http2.ConfigureTransport(tr)

    return &http.Client{
        Transport: tr,
    }
}



func init()  {
	config = Config{
		AWSAccessKey: os.Getenv("AWSAccessKey"),
		AWSSecretKey: os.Getenv("AWSSecretKey"),
		S3Region: os.Getenv("S3Region"),
		S3Bucket: os.Getenv("S3Bucket"),
		FileURL: os.Getenv("FileURL"),
	}

	creds := credentials.NewStaticCredentials(config.AWSAccessKey, config.AWSSecretKey, "")
	// sess = session.Must(session.NewSession(&aws.Config{
	// 	Region: aws.String(config.S3Region),
	// 	Credentials: creds,
	// }))

	sess = session.Must(session.NewSession(&aws.Config{
		HTTPClient: NewHTTPClientWithSettings(HTTPClientSettings{
    		Connect:          90 * time.Second,
    		ExpectContinue:   1 * time.Second,
    		IdleConn:         90 * time.Second,
    		ConnKeepAlive:    600 * time.Second,
    		MaxAllIdleConns:  700,
    		MaxHostIdleConns: 10,
    		ResponseHeader:   50 * time.Second,
    		TLSHandshake:     50 * time.Second,
			}),
		Region: aws.String(config.S3Region),
		Credentials: creds,
	}))
	// uploader = s3manager.NewUploader(sess)
	
    svc := s3.New(sess, aws.NewConfig().WithLogLevel(aws.LogDebugWithRequestRetries))
    xray.AWS(svc.Client)
}

func handler(ctx context.Context, sqsEvent events.SQSEvent) error {
	t := time.Now()
	var l []string
    for _, message := range sqsEvent.Records {
		if err := sync(message.Body); err != nil {
			log.Printf("sync %s to s3 error: %v", message.Body, err)
			return err
		}
		l = append(l, message.Body)
    }
	e := time.Now().Sub(t).Milliseconds()
	log.Printf("halder exec time %v, sqs info: %v", e, l)
    return nil
}

func sync(key string) error {
	startDownload := time.Now()
	// qnURL := storage.MakePublicURL(os.Getenv("QiniuDomain"), key)
	endT := time.Now().Sub(startDownload).Milliseconds()

	log.Printf("file make public url %s time: %v", "book.epub", endT)
	// qnURL := path.Join(config.QiniuDomain, key)
	res, err := http.Get(config.FileURL)
	if err != nil {
		// log.Print("download bjss3 resource error, again download.", err)
		return fmt.Errorf("download file resource error: %v", err)
	}
	defer res.Body.Close()
//
	out, err := os.Create("/tmp/tmpfile")
  	if err != nil {
    	log.Print("write to file failed, %v.", err)
  	}
  	defer out.Close()
  	io.Copy(out, res.Body)
  	
//
	endDownload := time.Now().Sub(startDownload).Milliseconds()
	log.Printf("Download bjss3 %s time: %v", key, endDownload)

	s3UploadStart := time.Now()
	
  // Open the file for use
    file, err := os.Open("/tmp/tmpfile")
    if err != nil {
        log.Print("read to file failed, %v.", err)
    }
    defer file.Close()
    // Get file size and read the file content into a buffer
    fileInfo, err := file.Stat()
    if err != nil {
        log.Print("read fileInfo failed, %v.", err)
    }
    var size int64 = fileInfo.Size()
    buffer := make([]byte, size)
    file.Read(buffer)
	
	log.Print("read fileInfo file size, %v.", size)

	ctx, cancelFn := context.WithTimeout(context.TODO(), 20*time.Second)
	defer cancelFn()
	_, err = svc.PutObjectWithContext(ctx, &s3.PutObjectInput{
 		Bucket: aws.String(config.S3Bucket),
 		Key:    aws.String(key),
 		Body:   bytes.NewReader(buffer),
 	})

 	log.Print("read fileInfo failed,marked2")

	if err != nil {
		// log.Print("Upload s3 error, again upload.", err)
		return fmt.Errorf("Upload s3 error: %v", err)
	}

	s3UploadEnd := time.Now().Sub(s3UploadStart).Milliseconds()
	log.Printf("Upload s3 %s time: %v", key, s3UploadEnd)

	endRequest := time.Now().Sub(startDownload).Milliseconds()
	log.Printf("Exec sync %s total time: %v", key, endRequest)
	return nil
}

func main() {
    lambda.Start(handler)
}