package blob

import (
	// "bytes"
	"bytes"
  // "context"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"os"
	"strings"
  "strconv"

  // "github.com/IBM/ibm-cos-sdk-go/feature/s3/manager"

	"github.com/IBM/ibm-cos-sdk-go/aws"
	"github.com/IBM/ibm-cos-sdk-go/aws/awserr"
	"github.com/IBM/ibm-cos-sdk-go/aws/credentials/ibmiam"
	"github.com/IBM/ibm-cos-sdk-go/aws/session"
	"github.com/IBM/ibm-cos-sdk-go/service/s3"
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
http://xxx.xxx.xxx.xxx/storage/blob/CreateBucket?bucket=XXXX
***/
func (b Blob) CreateBucket(res http.ResponseWriter, req *http.Request) {
  const paramsRequired int = 1
  params := req.URL.Query()
  if len(params) != paramsRequired {
    fmt.Fprintf(res, "Parameters required = 1; parameters provided = %d", len(params))
    return
  }
  var bucket string
  //Iterate over all the query parameters.
  for k, v := range params { //map[string][]string
    switch strings.ToLower(k) {
    case "bucket":
      bucket = v[0]
    default:
      fmt.Fprintf(res, "'%s' is an invalid parameter name.", k)
      return
    }
  }
  sess := session.Must(session.NewSession())
  client := s3.New(sess, conf)
  _, err := client.CreateBucket(&s3.CreateBucketInput {
    Bucket: aws.String(bucket),
  })
  if err != nil {
    fmt.Fprintf(res, "%v", err)
  }
}

/***
http://xxx.xxx.xxx.xxx/storage/blob/CreateBucket?bucket=XXXX
***/
func (b Blob) DeleteBucket(res http.ResponseWriter, req *http.Request) {
  const paramsRequired int = 1
  params := req.URL.Query()
  if len(params) != paramsRequired {
    fmt.Fprintf(res, "Parameters required = 1; parameters provided = %d", len(params))
    return
  }
  var bucket string
  //Iterate over all the query parameters.
  for k, v := range params { //map[string][]string
    switch strings.ToLower(k) {
    case "bucket":
      bucket = v[0]
    default:
      fmt.Fprintf(res, "'%s' is an invalid parameter name.", k)
      return
    }
  }
  sess := session.Must(session.NewSession())
  client := s3.New(sess, conf)
  _, err := client.DeleteBucket(&s3.DeleteBucketInput {
    Bucket: aws.String(bucket),
  })
  if err != nil {
    fmt.Fprintf(res, "%+v", err)
  }
}

/***
http://xxx.xxx.xxx.xxx/storage/blob/ListItemsInBucket?bucket=xxxx
***/
func (b Blob) ListItemsInBucket(res http.ResponseWriter, req *http.Request) {
  fmt.Printf("Listing items in bucket.\n")
  const paramsRequired int = 1
  params := req.URL.Query()
  if len(params) != paramsRequired {
    fmt.Fprintf(res, "Parameters required = 1; parameters provided = %d", len(params))
    fmt.Printf("Parameters required = 1; parameters provided = %d\n", len(params))
    return
  }
  var bucket string
  //Iterate over all the query parameters.
  for k, v := range params { //map[string][]string
    switch strings.ToLower(k) {
    case "bucket":
      bucket = v[0]
    default:
      fmt.Fprintf(res, "'%s' is an invalid parameter name.", k)
      fmt.Printf("'%s' is an invalid parameter name.\n", k)
      return
    }
  }
  sess := session.Must(session.NewSession())
  client := s3.New(sess, conf)
  var maxKeys int64 = 1_000  //Max items return.
  items, err := client.ListObjectsV2(&s3.ListObjectsV2Input{
    Bucket: aws.String(bucket),
    MaxKeys: &maxKeys,
  })
  if err != nil {
    fmt.Fprintf(res, "Bucket: %s\n%v", bucket, err)
    fmt.Printf("Bucket: %s\n%v", bucket, err)
  } else {
    fmt.Fprintf(res, "Found %d item(s) in bucket %s.\n", len(items.Contents), bucket)
    fmt.Printf("Found %d items in bucket %s\n", len(items.Contents), bucket)
    for i, item := range items.Contents {
      fmt.Fprintf(res, "%d.\t%s\t%s\t%d\t%s\n", i + 1, *item.Key, *item.LastModified, *item.Size,
                  *item.StorageClass)
    }
  }
}

/***
http://xxx.xxx.xxx.xxx/storage/blob/DeleteItemFromBucket?bucket=xxxx&item=xxx
***/
func (b Blob) DeleteItemFromBucket(res http.ResponseWriter, req *http.Request) {
  fmt.Printf("Deleting an item from a bucket.\n")
  const paramsRequired int = 2
  params := req.URL.Query()
  if len(params) != paramsRequired {
    fmt.Fprintf(res, "Parameters required = %d; parameters provided = %d", paramsRequired, len(params))
    fmt.Printf("Parameters required = %d; parameters provided = %d\n", paramsRequired, len(params))
    return
  }
  var bucket string
  var item string
  //Iterate over all the query parameters.
  for k, v := range params { //map[string][]string
    switch strings.ToLower(k) {
    case "bucket":
      bucket = v[0]
    case "item":
      item = v[0]
    default:
      fmt.Fprintf(res, "'%s' is an invalid parameter name.", k)
      fmt.Printf("'%s' is an invalid parameter name.\n", k)
      return
    }
  }
  sess := session.Must(session.NewSession())
  client := s3.New(sess, conf)
  /***
  Removes the null version (if there is one) of an object and inserts a delete marker, which
  becomes the latest version of the object. If there isn't a null version, Amazon S3 does not
  remove any objects. (If the object doesn't exist, it's not an error when calling DeleteObject.)
  ***/
  _, err := client.DeleteObject(&s3.DeleteObjectInput{
    Bucket: aws.String(bucket),
    Key: aws.String(item),
  })
  if err != nil {
    fmt.Fprintf(res, "%+v", err)
    fmt.Printf("Error when deleting item '%s' from bucket '%s': %+v\n", item, bucket, err)
  } else {
    fmt.Fprintf(res, "Item '%s' was deleted from bucket '%s'.\n", item, bucket)
    fmt.Printf("Item '%s' was deleted from bucket '%s'.\n", item, bucket)
  }
}

/***
http://XXX.XXX.XXX.XXX/storage/blob/DownloadBlobFile?bucket=xxxx&item=xxxx
***/
func (b Blob) DownloadBlobFile(res http.ResponseWriter, req *http.Request) {
  fmt.Printf("Downloading an item from a bucket.\n")
  const paramsRequired int = 2
  params := req.URL.Query()
  if len(params) != paramsRequired {
    fmt.Fprintf(res, "Parameters required = %d; parameters provided = %d", paramsRequired, len(params))
    fmt.Printf("Parameters required = %d; parameters provided = %d\n", paramsRequired, len(params))
    return
  }
  var bucket string
  var item string
  //Iterate over all the query parameters.
  for k, v := range params { //map[string][]string
    switch strings.ToLower(k) {
    case "bucket":
      bucket = v[0]
    case "item":
      item = v[0]
    default:
      fmt.Fprintf(res, "'%s' is an invalid parameter name.", k)
      fmt.Printf("'%s' is an invalid parameter name.\n", k)
      return
    }
  }
  sess := session.Must(session.NewSession())
  client := s3.New(sess, conf)
  result, err := client.GetObject(&s3.GetObjectInput{
    Bucket: aws.String(bucket),
    Key: aws.String(item),
  })
  if err != nil {
    if aerr, ok := err.(awserr.Error); ok {
      switch aerr.Code() {
      case s3.ErrCodeNoSuchKey:
        fmt.Printf("%+v\n", aerr.Error())
        fmt.Fprintf(res, aerr.Error(), s3.ErrCodeNoSuchKey)
      case s3.ErrCodeInvalidObjectState:
        fmt.Printf("%+v\n", aerr.Error())
        fmt.Fprintf(res, aerr.Error(), s3.ErrCodeInvalidObjectState)
      default:
        fmt.Printf("%+v\n", aerr.Error())
        fmt.Fprintf(res, aerr.Error())
      }
    } else {
      fmt.Printf("GetObject Error: %+v\n", err.Error())
      fmt.Fprintf(res, err.Error())
    }
    return
  }
  defer result.Body.Close()
  /***
  To make the browser open the download dialog, add a Content-Disposition and Content-Type headers
  to the response. Furthermore, to show proper progress, add the Content-Length header of the
  response.
  ***/
  res.Header().Set("Content-Disposition", "attachment; filename=" + strconv.Quote(item))
  res.Header().Set("Content-Type", *result.ContentType)
  res.Header().Set("Content-Length", fmt.Sprintf("%d", *result.ContentLength))
  //Stream the body to the client without fully loading it into memory.
  size, err := io.Copy(res, result.Body)
  if err != nil {
    fmt.Printf("Copy failed: %+v\n", err)
    fmt.Fprintf(res, "Copy failed: %+v", err)
  } else {
    fmt.Printf("Downloaded file %s successfully; sent=%d -> storage=%d.\n", item, size, *result.ContentLength)
    // fmt.Fprintf(res, "Downloaded file %s successfully; sent=%d -> storage=%d.", item, size, *result.ContentLength)
  }
}



/***
func emptyBucket(service *s3.S3, bucketName string) {
	objs, err := service.ListObjects(&s3.ListObjectsInput{Bucket: stringPtr(bucketName)})
	if err != nil {
		panic(err)
	}

	for _, o := range objs.Contents {
		_, err := service.DeleteObject(&s3.DeleteObjectInput{Bucket: stringPtr(bucketName), Key: o.Key})
		if err != nil {
			panic(err)
		}
	}
}
***/

/***
http://XXX.XXX.XXX.XXX/storage/blob/UploadBlobFile?bucket=xxxx
***/
func (b Blob) UploadBlobFile(res http.ResponseWriter, req *http.Request) {
  const paramsRequired int = 1
  params := req.URL.Query()
  if len(params) != paramsRequired {
    fmt.Fprintf(res, "Parameters required = %d; parameters provided = %d", paramsRequired, len(params))
    fmt.Printf("Parameters required = %d; parameters provided = %d\n", paramsRequired, len(params))
    return
  }
  var bucket string
  //Iterate over all the query parameters.
  for k, v := range params { //map[string][]string
    switch strings.ToLower(k) {
    case "bucket":
      bucket = v[0]
    default:
      fmt.Fprintf(res, "'%s' is an invalid parameter name.", k)
      fmt.Printf("'%s' is an invalid parameter name.\n", k)
      return
    }
  }
  //
  if req.Method != http.MethodPost {
    fmt.Println("Uploading a blob file...")
    fmt.Fprint(res,
      `<html>
       <head>
         <title>Upload Blob File</title>
       </head>
       <body>
         <p>Upload a file to storage:</p>
         <form method="POST" enctype="multipart/form-data">
           <div style="float:left;">
             <input type="file" name="uploadfile">
           </div>
           <br><br><br>
           <div style="float:center;">
             <input type="submit" value="Upload" style="height:40px; width:200px">
           </div>
         </form>
       </body>
       </html>`)
    return
  }


  //MaxBytesReader prevents clients from accidentally or maliciously sending a large request and wasting server resources. If possible, it tells the ResponseWriter to close the connection after the limit has been reached.
  req.Body = http.MaxBytesReader(res, req.Body, 100 << 20)
  defer req.Body.Close()
  // defer func() {
  //   if r := recover(); r != nil {
  //     http.Error(res, "Violation of max bytes.", http.StatusInternalServerError)
  //     fmt.Printf("Multipartyyy form: %+v\n", r)
  //     return
  //   }
  // }()


  // n << x = n * 2^x
  // n << x = n / 2^x
  err := req.ParseMultipartForm(32 << 20)  //32MB in memory, rest on disk.
  if err != nil {
    if err == io.ErrUnexpectedEOF {
      fmt.Printf("Request body too large (Multipartxxx form): %+v\n", err)
      http.Error(res, err.Error(), http.StatusRequestEntityTooLarge)
    } else {
      fmt.Printf("Requestaaa body too large (Multipartxxx form): %+v\n", err)
      http.Error(res, err.Error(), http.StatusInternalServerError)
    }
    return
  }

  // FormFile returns the first file for the given key `uploadfile`
  // it also returns the FileHeader so we can get the Filename,
  // the Header and the size of the file
  infile, handler, err := req.FormFile("uploadfile")
  if err != nil {
    //fmt.Fprintln(res, err)
    http.Error(res, err.Error(), http.StatusInternalServerError)
    fmt.Printf("Error while parsing the form parameters: %+v", err)
    return
  }
  defer infile.Close()
  fmt.Printf("File Name: %+v\n", handler.Filename)
  fmt.Printf("File Size: %+v\n", handler.Size)
  fmt.Printf("MIME Header: %+v\n", handler.Header)
  
  
  // read all of the contents of our uploaded file into a
  // byte array
  fileBytes, err := ioutil.ReadAll(infile)
  if err != nil {
    fmt.Println(err)
  }
  
  
  //The http.DetectContentType function only needs the first 512 bytes of the file to detect its
  //file type based on the mimesniff algorithm.
  detectedFileType := http.DetectContentType(fileBytes)
  switch detectedFileType {
  case "image/jpeg", "image/jpg":
  case "image/gif", "image/png":
  case "video/mp4":
  case "application/pdf":
    break
  default:
    fmt.Println("Invalid file type.", http.StatusBadRequest)
    http.Error(res, "Invalid file type.", http.StatusBadRequest)
    return
  }
  





  sess := session.Must(session.NewSession())
  client := s3.New(sess, conf)
  _, err = client.PutObject(&s3.PutObjectInput{
    Bucket: aws.String(bucket),
    Key: aws.String(handler.Filename),
    // ACL: aws.String("private"),
    Body: bytes.NewReader(fileBytes),
    ContentLength: aws.Int64(handler.Size),
    ContentType: aws.String(handler.Header.Get("Content-Type")),
    ContentDisposition: aws.String("attachment"),
    ServerSideEncryption: aws.String("AES256"),
  })
  if err != nil {
    fmt.Println(err)
    fmt.Fprintln(res, err)
  } else {
    fmt.Printf("File %s with size %d was uploaded.", handler.Filename, handler.Size)
  }
}








/***
import (
	"crypto/rand"
)
func randToken(len int) string {
	b := make([]byte, len)
	rand.Read(b)
	return fmt.Sprintf("%x", b)
}
***/