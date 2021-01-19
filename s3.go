package main

import (
	"fmt"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"log"
	"net/url"
	"os"
	"path/filepath"
	"strings"
)


type fileWalk chan string

func (f fileWalk) Walk(path string, info os.FileInfo, err error) error {
	if err != nil {
		return err
	}
	if !info.IsDir() {
		f <- path
	}
	return nil
}


func CreateS3Client() (*s3.S3, *session.Session) {
	//$ export AWS_ACCESS_KEY_ID=YOUR_AKID
	//$ export AWS_SECRET_ACCESS_KEY=YOUR_SECRET_KEY
	//$ export AWS_ENDPOINT=http://s3.cn-northwest-1.amazonaws.com.cn

	sess, err := session.NewSession(&aws.Config{
		Region: aws.String("cn-northwest-1")},
	)
	if err != nil {
		panic(err)
	}
	aws_endpoint := os.Getenv("AWS_ENDPOINT")
	svc := s3.New(sess, &aws.Config{Endpoint: aws.String(aws_endpoint)})
	return svc, sess
}

func ListS3Bucket(sess *session.Session, svc *s3.S3) {
	bk_result, err := svc.ListBuckets(nil)
	if err != nil {
		exitErrorf("Unable to list buckets, %v", err)
	}

	fmt.Println("Buckets:")
	for _, b := range bk_result.Buckets {
		fmt.Printf("* %s created on %s\n",
			aws.StringValue(b.Name), aws.TimeValue(b.CreationDate))
	}
}

func UploadS3Dir(sess *session.Session, svc *s3.S3, src string, dst string) {
	dstUrl, err := url.Parse(dst)
	if err != nil {
		panic(err)
	}
	bucket := dstUrl.Host
	targetPath := strings.TrimPrefix(dstUrl.Path, "/")
	fmt.Println("o", src, dst, bucket, targetPath)

	walker := make(fileWalk)
	go func() {
		// Gather the files to upload by walking the path recursively
		if err := filepath.Walk(src, walker.Walk); err != nil {
			log.Fatalln("Walk failed:", err)
		}
		close(walker)
	}()

	// For each file found walking, upload it to S3
	uploader := s3manager.NewUploader(sess)
	for path := range walker {
		rel, err := filepath.Rel(src, path)
		if err != nil {
			log.Fatalln("Unable to get relative path:", path, err)
		}
		file, err := os.Open(path)
		if err != nil {
			log.Println("Failed opening file", path, err)
			continue
		}
		defer file.Close()
		result, err := uploader.Upload(&s3manager.UploadInput{
			Bucket: &bucket,
			Key:    aws.String(filepath.Join(targetPath, rel)),
			Body:   file,
		})
		if err != nil {
			log.Fatalln("Failed to upload", path, err)
		}
		log.Println("Uploaded", path, result.Location)
	}
}

func DownloadS3Dir(sess *session.Session, svc *s3.S3, src string, dst string) {
	srcUrl, err := url.Parse(src)
	if err != nil {
		log.Fatal(err)
		panic(err)
	}
	bucket := srcUrl.Host
	path := strings.TrimPrefix(srcUrl.Path, "/")
	obj_result, err := svc.ListObjectsV2(&s3.ListObjectsV2Input{Bucket: aws.String(bucket), Prefix: aws.String(path), Delimiter: aws.String("/")})
	if err != nil {
		exitErrorf("Unable to list objects, %v, %s, %s", err, src, dst)
	}
	log.Println("Objects:")
	downloader := s3manager.NewDownloader(sess)

	for _, o := range obj_result.Contents {
		fmt.Printf("* %s %s %s\n",
			aws.StringValue(o.Key), aws.StringValue(o.ETag), aws.TimeValue(o.LastModified))

		item := aws.StringValue(o.Key)
		item = strings.TrimPrefix(item, path)
		item = filepath.Join(dst, item)
		file, err := os.Create(item)
		if err != nil {
			os.MkdirAll(filepath.Dir(item), os.ModePerm)
			file, err = os.Create(item)
			if err != nil {
				exitErrorf("Unable to open file %q, %v", item, err)
			}
		}
		defer file.Close()

		numBytes, err := downloader.Download(file,
			&s3.GetObjectInput{
				Bucket: aws.String(bucket),
				Key:    o.Key,
			})
		if err != nil {
			exitErrorf("Unable to download item %q, %v", item, err)
		}

		fmt.Println("Downloaded", file.Name(), numBytes, "bytes")
	}
}
