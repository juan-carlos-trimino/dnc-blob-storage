package blob

import (
	// "bytes"
	"bytes"
  "fmt"
  "io/ioutil"
	"net/http"
	"os"
	"strings"

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
  fmt.Printf("Listing items in bucket.\n")
  const paramsRequired int = 1
  params := req.URL.Query()
  if len(params) != paramsRequired {
    fmt.Fprintf(res, "Parameters required = 1; parameters provided = %d", len(params))
    fmt.Printf("Parameters required = 1; parameters provided = %d\n", len(params))
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
    fmt.Fprintf(res, "Found %d items in bucket %s", len(items.Contents), bucket)
    fmt.Printf("Found %d items in bucket %s\n", len(items.Contents), bucket)
    for i, item := range items.Contents {
      fmt.Fprintf(res, "%d.\t%s\t%s\t%d\t%s\n", i + 1, *item.Key, *item.LastModified, *item.Size,
                  *item.StorageClass)
    }
  }
}




/***
http://XXX.XXX.XXX.XXX/storage/blob/UploadBlobFile?bucket=xxxx
***/
func (b Blob) UploadBlobFile(res http.ResponseWriter, req *http.Request) {
  const paramsRequired int = 1
  params := req.URL.Query()
  if len(params) != paramsRequired {
    fmt.Fprintf(res, "Parameters required = 1; parameters provided = %d", len(params))
    fmt.Printf("Parameters required = 1; parameters provided = %d\n", len(params))
    return
  }
  var bucket string
  var err = error(nil)
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
  if req.Method != "POST" {
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
             <input type="submit" value="Upload" style="height:50px; width:100px">
           </div>
         </form>
       </body>
       </html>`)
    return
  }

    // Parse our multipart form, 10 << 20 specifies a maximum
    // upload of 10 MB files.
    // r.ParseMultipartForm(10 << 20)

  //err = req.ParseMultipartForm(10 << 20)  //32MB in memory, rest on disk.
  if err != nil {
    http.Error(res, err.Error(), http.StatusInternalServerError)
    fmt.Printf("Error 1: %v", err)
    return
  }
  // FormFile returns the first file for the given key `uploadfile`
  // it also returns the FileHeader so we can get the Filename,
  // the Header and the size of the file
  infile, handler, err := req.FormFile("uploadfile")
  if err != nil {
    //fmt.Fprintln(res, err)
    http.Error(res, err.Error(), http.StatusInternalServerError)
    fmt.Printf("Error 2: %v", err)
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

