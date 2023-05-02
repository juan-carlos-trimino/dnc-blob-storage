package blob

import (
	"fmt"
	"github.com/IBM/ibm-cos-sdk-go/aws"
	"github.com/IBM/ibm-cos-sdk-go/aws/awserr"
	"github.com/IBM/ibm-cos-sdk-go/aws/credentials/ibmiam"
	"github.com/IBM/ibm-cos-sdk-go/aws/session"
	"github.com/IBM/ibm-cos-sdk-go/service/s3"
	"net/http"
	"os"
	"strings"
)

type Blob struct{}

//Initialize configuration.
var conf = aws.NewConfig().
           WithRegion(os.Getenv("REGION")).
           WithEndpoint(os.Getenv("ENDPOINT")).
           WithCredentials(ibmiam.NewStaticCredentials(aws.NewConfig(),
            "https://iam.cloud.ibm.com/identity/token", os.Getenv("API_KEY"),
            os.Getenv("SERVICE_INSTANCE_ID"))).
          WithS3ForcePathStyle(true)

//https://cloud.ibm.com/docs/cloud-object-storage?topic=cloud-object-storage-using-go

//https://cloud.ibm.com/docs/cloud-object-storage/iam?topic=cloud-object-storage-service-credentials
//https://docs.aws.amazon.com/sdk-for-go/api/service/s3/#S3.ListBuckets
//https://ibm.github.io/ibm-cos-sdk-go/service/s3/ListBucketsInput.html

/***
http://xxx.xxx.xxx.xxx/storage/blob/ListBuckets
***/
func (b Blob) ListBuckets(res http.ResponseWriter, req *http.Request) {
  sess := session.Must(session.NewSession())
  client := s3.New(sess, conf)
  buckets, err := client.ListBuckets(&s3.ListBucketsInput{})
  if err != nil {
    if aerr, ok := err.(awserr.Error); ok {
      switch aerr.Code() {
      default:
        fmt.Fprintf(res, aerr.Error())
      }
    } else {
      //Print the error, cast err to awserr.Error to get the Code and Message from an error.
      fmt.Fprintln(res, err.Error())
    }
  } else {
    for i, bucket := range buckets.Buckets {
      fmt.Fprintf(res, "%d.\t%s\t\t%s\n", i + 1, *bucket.Name, *bucket.CreationDate)
    }
  }
}

/***
http://xxx.xxx.xxx.xxx/storage/blob/CreateBucket?name=XXXX
***/
func (b Blob) CreateBucket(res http.ResponseWriter, req *http.Request) {
  const paramsRequired int = 1
  params := req.URL.Query()
  if len(params) != paramsRequired {
    fmt.Fprintf(res, "Parameters required = 1; parameters provided = %d", len(params))
    return
  }
  var bucket string
  var err = error(nil)
  //Iterate over all the query parameters.
  for k, v := range params { //map[string][]string
    switch strings.ToLower(k) {
    case "name":
      bucket = v[0]
    default:
      fmt.Fprintf(res, "'%s' is an invalid parameter name.", k)
      return
    }
  }
  sess := session.Must(session.NewSession())
  client := s3.New(sess, conf)
  _, err = client.CreateBucket(&s3.CreateBucketInput {
    Bucket: aws.String(bucket),
  })
  if err != nil {
    fmt.Fprintf(res, "%v", err)
  }
}

/***
http://xxx.xxx.xxx.xxx/storage/blob/CreateBucket?name=XXXX
***/
func (b Blob) DeleteBucket(res http.ResponseWriter, req *http.Request) {
  const paramsRequired int = 1
  params := req.URL.Query()
  if len(params) != paramsRequired {
    fmt.Fprintf(res, "Parameters required = 1; parameters provided = %d", len(params))
    return
  }
  var bucket string
  var err = error(nil)
  //Iterate over all the query parameters.
  for k, v := range params { //map[string][]string
    switch strings.ToLower(k) {
    case "name":
      bucket = v[0]
    default:
      fmt.Fprintf(res, "'%s' is an invalid parameter name.", k)
      return
    }
  }
  sess := session.Must(session.NewSession())
  client := s3.New(sess, conf)
  _, err = client.DeleteBucket(&s3.DeleteBucketInput {
    Bucket: aws.String(bucket),
  })
  if err != nil {
    fmt.Fprintf(res, "%v", err)
  }
}

/***
http://xxx.xxx.xxx.xxx/storage/blob/ListItemsInBucket?name=xxxx
***/
func (b Blob) ListItemsInBucket(res http.ResponseWriter, req *http.Request) {
  const paramsRequired int = 1
  params := req.URL.Query()
  if len(params) != paramsRequired {
    fmt.Fprintf(res, "Parameters required = 1; parameters provided = %d", len(params))
    return
  }
  var bucket string
  var err = error(nil)
  //Iterate over all the query parameters.
  for k, v := range params { //map[string][]string
    switch strings.ToLower(k) {
    case "name":
      bucket = v[0]
    default:
      fmt.Fprintf(res, "'%s' is an invalid parameter name.", k)
      return
    }
  }
  sess := session.Must(session.NewSession())
  client := s3.New(sess, conf)
  var maxKeys int64 = 1_000  //Max items return.
  items, err := client.ListObjectsV2(&s3.ListObjectsV2Input {
    Bucket: aws.String(bucket),
    MaxKeys: &maxKeys,
  })
  if err != nil {
    fmt.Fprintf(res, "%v", err)
  } else {
    fmt.Fprintf(res, "Found %d items in bucket %s", len(items.Contents), bucket)
    for i, item := range items.Contents {
      fmt.Fprintf(res, "%d.\t%s\t%s\t%d\t%s\n", i + 1, *item.Key, *item.LastModified, *item.Size,
                  *item.StorageClass)
    }
  }
}




