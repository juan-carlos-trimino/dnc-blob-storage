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
	// "strings"
)

type Blob struct{}

// //For services that use HMAC credentials for authentication.
// type hmacType struct {
//   apiVersion string
//   accessKeyId string
//   secretAccessKey string
//   region string
//   endpoint string
// }

// //For services that use IAM authentication.
// type iamType struct {
//   apiVersion string
//   apiKeyId string
//   serviceInstanceId string
//   endpoint string
// }


//https://cloud.ibm.com/docs/cloud-object-storage?topic=cloud-object-storage-using-go

//https://cloud.ibm.com/docs/cloud-object-storage/iam?topic=cloud-object-storage-service-credentials
//https://docs.aws.amazon.com/sdk-for-go/api/service/s3/#S3.ListBuckets
//https://ibm.github.io/ibm-cos-sdk-go/service/s3/ListBucketsInput.html

func (b Blob) ListBuckets(res http.ResponseWriter, req *http.Request) {
  //Initialize configuration.
  conf := aws.NewConfig().
          WithRegion("us-south").
          WithEndpoint(os.Getenv("ENDPOINT")).
          WithCredentials(ibmiam.NewStaticCredentials(aws.NewConfig(),
            "https://iam.cloud.ibm.com/identity/token", os.Getenv("API_KEY"),
            os.Getenv("SERVICE_INSTANCE_ID"))).
          WithS3ForcePathStyle(true)
  sess := session.Must(session.NewSession())


  // sess, err := session.NewSessionWithOptions(session.Options{
	// 	Profile: "default",
	// 	Config: aws.Config{
	// 		Region: aws.String("us-south"),
	// 	},
	// })


  // client := s3.New(session.Must(session.NewSession(&aws.Config{
	// 	Region: aws.String("us-south"),
	// })))

	// if err != nil {
	// 	fmt.Printf("Failed to initialize new session: %v", err)
	// 	return
	// }

  // sess, err := session.NewSession()
  // if err != nil {
  //   fmt.Fprintf(res, "Failed to initialize new session: %v", err)
  //   return
  // }
  client := s3.New(sess, conf)
  // client := s3.New(sess)
  buckets, err := client.ListBuckets(&s3.ListBucketsInput{})
  // if err != nil {
  //   fmt.Fprintf(res, "Failed to list the buckets:\n%v", err)
  //   return
  // } else {
  //   for _, bucket := range buckets.Buckets {
  //     fmt.Fprintf(res, "Bucket: %s, created at: %s\n", *bucket.Name, *bucket.CreationDate)
  //   }
  // }
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
  // } else if result != nil {
  //   fmt.Fprintf(res, "List:\n%s", result)
  // } else {
  //   fmt.Fprintf(res, "List:\nThere are no buckets.")
  // }
  } else {
    // i := 0
    for i, bucket := range buckets.Buckets {
      fmt.Fprintf(res, "%d\t\t%s\t\t%s\n", i, *bucket.Name, *bucket.CreationDate)
    }
  }

}



