//IBM COS.
package main

import (
  "context"
  "errors"
  "fmt"
  "net/http"
  "os"
  "os/signal"
  "storage/blob"
  "strconv"
  "strings"
  "syscall"
  "time"
)

//Environment variables.
// var MAX_RETRIES int = 10
var PORT string = "8080"
var SHUTDOWN_TIMEOUT int = 15
var SVC_NAME string
var BUCKET_NAME string
var ENDPOINT string
var AUTHENTICATION_TYPE string
var REGION string
var SECRET_ACCESS_KEY string
var ACCESS_KEY_ID string
var API_KEY string
var SERVICE_INSTANCE_ID string

type handlers struct{
  mux map[string]func(http.ResponseWriter, *http.Request)
}

func (h *handlers) ServeHTTP(res http.ResponseWriter, req *http.Request) {
  if handler, ok := h.mux[req.URL.Path]; ok {
    handler(res, req)
    return
  }
  http.NotFound(res, req) //404 - page not found
}

func main() {
  var exists bool = false
  SVC_NAME, exists = os.LookupEnv("SVC_NAME")
  if !exists {
    fmt.Println("Missing environment variable: SVC_NAME")
    return
  }
  //
  BUCKET_NAME, exists = os.LookupEnv("BUCKET_NAME")
  if !exists {
    fmt.Println("Missing environment variable: BUCKET_NAME.")
    return
  }
  //
  ENDPOINT, exists = os.LookupEnv("ENDPOINT")
  if !exists {
    fmt.Println("Missing environment variable: ENDPOINT.")
    return
  }
  //
  AUTHENTICATION_TYPE, exists = os.LookupEnv("AUTHENTICATION_TYPE")
  if !exists {
    fmt.Println("Missing environment variable: AUTHENTICATION_TYPE.")
    return
  }
  //
  REGION, exists = os.LookupEnv("REGION")
  if !exists {
    fmt.Println("Missing environment variable: REGION.")
    return
  }
  //
  if strings.EqualFold(AUTHENTICATION_TYPE, "hmac") {
    // REGION, exists = os.LookupEnv("REGION")
    // if !exists {
    //   fmt.Println("Missing environment variable: REGION.")
    //   return
    // }
    //
    SECRET_ACCESS_KEY, exists = os.LookupEnv("SECRET_ACCESS_KEY")
    if !exists {
      fmt.Println("Missing environment variable: SECRET_ACCESS_KEY.")
      return
    }
    //
    ACCESS_KEY_ID, exists = os.LookupEnv("ACCESS_KEY_ID")
    if !exists {
      fmt.Println("Missing environment variable: ACCESS_KEY_ID.")
      return
    }
  } else if strings.EqualFold(AUTHENTICATION_TYPE, "iam") {
    API_KEY, exists = os.LookupEnv("API_KEY")
    if !exists {
      fmt.Println("Missing environment variable: API_KEY.")
      return
    }
    //
    SERVICE_INSTANCE_ID, exists = os.LookupEnv("SERVICE_INSTANCE_ID")
    if !exists {
      fmt.Println("Missing environment variable: SERVICE_INSTANCE_ID.")
      return
    }
  } else {
    fmt.Printf("Unknown authentication type (AUTHENTICATION_TYPE): %s\n", AUTHENTICATION_TYPE)
    return
  }
  //
  _, exists = os.LookupEnv("PORT")
  if exists {
    PORT = os.Getenv("PORT")
  }
  fmt.Printf("Using PORT: %s\n", PORT)
  SVC_NAME += ":" + PORT
  //
  _, exists = os.LookupEnv("SHUTDOWN_TIMEOUT")
  if exists {
    sdto := os.Getenv("SHUTDOWN_TIMEOUT")
    tm, err := strconv.Atoi(sdto)
    if err != nil {
      fmt.Printf("'%s' is not an int number.\n", sdto)
    }
    SHUTDOWN_TIMEOUT = tm
  }
  fmt.Printf("Using SHUTDOWN_TIMEOUT: %d\n", SHUTDOWN_TIMEOUT)
  //
  var h handlers = handlers{}
  h.mux = make(map[string]func(http.ResponseWriter, *http.Request), 8)
  h.mux["/readiness"] =
  func (res http.ResponseWriter, req *http.Request) {
    fmt.Printf("\naaaaaaServer not ready. %s\n", SVC_NAME)
    // req, err := http.NewRequest(http.MethodHead, SERVER, nil)
    // if err != nil {
    //   fmt.Println("Server not ready.")
    //   res.WriteHeader(http.StatusInternalServerError)
    //   return
    // }
    // resp, err := client.Do(req)
    // if err != nil {
    //   fmt.Printf("client: error making http request: %s\n", err)
    //   res.WriteHeader(http.StatusInternalServerError)
    //   return
    // }
    // resp.Body.Close()
    // fmt.Println("Server is ready.")
    // //https://go.dev/src/net/http/status.go
    res.WriteHeader(http.StatusOK)
  }
  var b blob.Blob
  h.mux["/storage/blob/ListBuckets"] = b.ListBuckets
  h.mux["/storage/blob/CreateBucket"] = b.CreateBucket
  h.mux["/storage/blob/DeleteBucket"] = b.DeleteBucket
  h.mux["/storage/blob/ListItemsInBucket"] = b.ListItemsInBucket
  h.mux["/storage/blob/UploadBlobFile"] = b.UploadBlobFile
  h.mux["/storage/blob/DeleteItemFromBucket"] = b.DeleteItemFromBucket
  h.mux["/storage/blob/DownloadBlobFile"] = b.DownloadBlobFile
  server := &http.Server {
    /***
    By not specifying an IP address before the colon, the server will listen on every IP address
    associated with the computer, and it will listen on port PORT.
    ***/
    Addr: ":" + PORT,
    Handler: &h,
  }
  /***
  A channel is a communication mechanism that lets one goroutine send values to another goroutine.
  Each channel is a conduit for values of a particular type, called the channel's element type.

  As with maps, a channel is a reference to the data structure created by make. When we copy a
  channel or pass one as an argument to a function, we are copying a reference, so caller and
  callee refer to the same data structure. As with other reference types, the zero value of a
  channel is nil.
  ***/
  signalChan := make(chan os.Signal, 1) //Buffered channel capacity 1; notifier will not block.
  /***
  When Shutdown is called, Serve, ListenAndServe, and ListenAndServeTLS immediately return
  ErrServerClosed. Make sure the program doesn't exit and waits instead for Shutdown to return.
  ***/
  waitMainChan := make(chan struct{})
  go func() {
    /***
    signal.Notify disables the default behavior for a given set of asynchronous signals and instead
    delivers them over one or more registered channels.
    https://pkg.go.dev/os/signal#hdr-Default_behavior_of_signals_in_Go_programs
    ***/
    signal.Notify(signalChan,
      syscall.SIGINT, //Ctrl-C
      syscall.SIGTERM, //Kubernetes sends a SIGTERM.
    )
    <- signalChan //Waiting for the signal; signal is discarded.
    ctx, cancel := context.WithTimeout(context.Background(), time.Duration(SHUTDOWN_TIMEOUT) * time.Second)
    defer func() {
      //Extra handling goes here...
      close(signalChan)
      //Calling cancel() releases the resources associated with the context.
      cancel()
      close(waitMainChan) //Shutdown is done; let the main goroutine terminate.
    }()
    //https://pkg.go.dev/net/http#Server.Shutdown
    if err := server.Shutdown(ctx); err != nil {
      fmt.Printf("Server shutdown failed: %+v", err) //https://pkg.go.dev/fmt
    }
  }()
  fmt.Printf("%s - Starting the server at %s...\n", time.Now().UTC().Format(time.RFC3339Nano), SVC_NAME)
  /***
  ListenAndServe runs forever, or until the server fails (or fails to start) with an error,
  always non-nil, which it returns.

  The web server invokes each handler in a new goroutine, so handlers must take precautions such as
  locking when accessing variables that other goroutines, including other requests to the same
  handler, may be accessing.
  ***/
  err := server.ListenAndServe()
  if errors.Is(err, http.ErrServerClosed) {
    fmt.Println("Server has been closed.")
  } else if err != nil {
    fmt.Printf("Server error: %s\n", "err")
    signalChan <- syscall.SIGINT //Let the goroutine finish.
  }
  <- waitMainChan //Block until shutdown is done.
  return
}

