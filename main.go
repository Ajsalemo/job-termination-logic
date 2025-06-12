package main

import (
	"bytes"
	"context"
	"os"
	"strings"

	// "fmt"
	"os/signal"
	"syscall"
	"time"

	// "github.com/google/uuid"
	"go.uber.org/zap"

	"github.com/Azure/azure-sdk-for-go/sdk/azidentity"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"
)

func init() {
	zap.ReplaceGlobals(zap.Must(zap.NewProduction()))
}

func handleError(err error) {
	if err != nil {
		zap.L().Fatal("An error occurred", zap.Error(err))
	}
}

// func uploadBlobs(url string, containerName string, ctx context.Context, credential *azidentity.DefaultAzureCredential) {
// 	// Create a client
// 	client, err := azblob.NewClient(url, credential, nil)
// 	handleError(err)

// 	for i := range 10 {
// 		blobName := fmt.Sprintf("blob-%s%s", uuid.NewString(), ".txt")
// 		data := []byte("This is blob data for " + blobName)
// 		zap.L().Info("Sleeping for 1 second before uploading blob", zap.Int("iteration", i))
// 		time.Sleep(1 * time.Second)
// 		zap.L().Info("Uploading blob", zap.String("blob name", blobName))
// 		// Upload a blob
// 		_, err = client.UploadBuffer(ctx, containerName, blobName, data, &azblob.UploadBufferOptions{})
// 		handleError(err)
// 	}
// }

var lastAccessBlob string

// Iterate over blobs - currently this just lists them out
func manageBlobs(url string, containerName string, ctx context.Context, credential *azidentity.DefaultAzureCredential) {
	// Create a client
	client, err := azblob.NewClient(url, credential, nil)
	handleError(err)

	pager := client.NewListBlobsFlatPager(containerName, nil)		
	for pager.More() {
		resp, err := pager.NextPage(ctx)
		handleError(err)
		for _, blob := range resp.Segment.BlobItems {
			time.Sleep(2 * time.Second)
			zap.L().Info("Sleeping for 2 seconds before logging blob data")
			zap.L().Info("Blob found", zap.String("name", *blob.Name), zap.Time("last modified", *blob.Properties.LastModified))
			lastAccessBlob = *blob.Name
		}
	}
}

// Check f or an existing checkpoint blob
func updateCheckpointForBlob(url string, ctx context.Context, credential *azidentity.DefaultAzureCredential) {
	zap.L().Info("Last accessed blob", zap.String("blob name", lastAccessBlob))
	// Create a client
	containerName := "checkpoint"
	client, err := azblob.NewClient(url, credential, nil)
	handleError(err)

	// Check if the checkpoint blob exists
	checkpointBlobName := "checkpoint.txt"
	_, err = client.DownloadStream(ctx, containerName, checkpointBlobName, nil)
	if err != nil {
		if strings.Contains(err.Error(), "BlobNotFound") {
			zap.L().Warn("Checkpoint blob does not exist, creating it", zap.String("blob name", checkpointBlobName))
			// Create the checkpoint blob with the last accessed blob name
			_, err = client.UploadBuffer(ctx, containerName, checkpointBlobName, []byte(lastAccessBlob), &azblob.UploadBufferOptions{})
			handleError(err)
			zap.L().Info("Checkpoint blob created successfully", zap.String("blob name", checkpointBlobName))
		} else {
			handleError(err)
		}
	} else {
		zap.L().Info("Checkpoint blob exists, updating it", zap.String("blob name", checkpointBlobName))
		// Update the checkpoint blob with the last accessed blob name
		_, err = client.UploadBuffer(ctx, containerName, checkpointBlobName, []byte(lastAccessBlob), &azblob.UploadBufferOptions{})
		handleError(err)
	}
	zap.L().Info("Checkpoint updated successfully", zap.String("blob name", checkpointBlobName))
}

func getLastCheckpointForBlob(url string, ctx context.Context, credential *azidentity.DefaultAzureCredential) {
	// Create a client
	containerName := "checkpoint"
	client, err := azblob.NewClient(url, credential, nil)
	handleError(err)
	// Download the checkpoint blob
	checkpointBlobName := "checkpoint.txt"
	downloadedCheckpointBlob := bytes.Buffer{}
	res, err := client.DownloadStream(ctx, containerName, checkpointBlobName, nil)
	// Check if the checkpoint blob exists
	if err != nil {
		if strings.Contains(err.Error(), "BlobNotFound") {
			zap.L().Warn("No checkpoint exists!", zap.String("blob name", checkpointBlobName))
		} else {
			handleError(err)
		}
	} else {
		retryReader := res.NewRetryReader(ctx, &azblob.RetryReaderOptions{})
		_, err = downloadedCheckpointBlob.ReadFrom(retryReader)
		handleError(err)
		err = retryReader.Close()
		handleError(err)

		zap.L().Info("Checkpoint blob downloaded successfully", zap.String("blob name", downloadedCheckpointBlob.String()))
		lastAccessBlob = downloadedCheckpointBlob.String()
		zap.L().Info("Last accessed blob from checkpoint", zap.String("blob name", lastAccessBlob))
	}
}

func main() {
	url := "https://ansalemostorage.blob.core.windows.net/"
	containerName := "general"
	ctx := context.Background()
	// Create credentials
	credential, err := azidentity.NewDefaultAzureCredential(nil)
	handleError(err)
	zap.L().Info("Checking for an existing blob checkpoint prior to managing blobs..")
	// Check for an existing checkpoint early on
	getLastCheckpointForBlob(url, ctx, credential)
	zap.L().Info("Application started, waiting for signals to shutdown gracefully..")
	// Notify the application of the below signals to be handled on shutdown
	s := make(chan os.Signal, 1)
	signal.Notify(s,
		syscall.SIGINT,
		syscall.SIGTERM,
		syscall.SIGQUIT)
	// Goroutine to clean up prior to shutting down
	go func() {
		sig := <-s
		switch sig {
		case os.Interrupt:
			zap.L().Warn("CTRL+C / os.Interrupt recieved, shutting down the application..")
			// Update last checkpoint for the blob
			updateCheckpointForBlob(url, ctx, credential)
			os.Exit(0)
		case syscall.SIGTERM:
			zap.L().Warn("SIGTERM recieved.., shutting down the application..")
			// Update last checkpoint for the blob
			updateCheckpointForBlob(url, ctx, credential)
			os.Exit(0)
		case syscall.SIGQUIT:
			zap.L().Warn("SIGQUIT recieved.., shutting down the application..")
			// Update last checkpoint for the blob
			updateCheckpointForBlob(url, ctx, credential)
			os.Exit(0)
		case syscall.SIGINT:
			zap.L().Warn("SIGINT recieved.., shutting down the application..")
			// Update last checkpoint for the blob
			updateCheckpointForBlob(url, ctx, credential)
			os.Exit(0)
		}
	}()
	// Upload blobs
	// uploadBlobs(url, containerName, ctx, credential)
	// Manage blobs
	manageBlobs(url, containerName, ctx, credential)
	// Update last checkpoint for the blob
	// This will run after manageBlobs() completes - this would be a "typical" succesful application run
	updateCheckpointForBlob(url, ctx, credential)
}
