package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	s3types "github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sts"
	"github.com/schollz/progressbar/v3"
	"golang.org/x/sync/semaphore"
)

// Configuration
const (
	MaxConcurrency = 100          // Adjust this based on your environment
	AwsRegion      = "ap-northeast-1" // AWS Region
	BatchSize      = 10           // Batch size
)

// S3Event struct defines the structure of S3 event notifications
type S3Event struct {
	Records []S3EventRecord `json:"Records"`
}

type S3EventRecord struct {
	AwsRegion         string                 `json:"awsRegion"`
	EventName         string                 `json:"eventName"`
	EventSource       string                 `json:"eventSource"`
	EventTime         string                 `json:"eventTime"`
	EventVersion      string                 `json:"eventVersion"`
	RequestParameters map[string]interface{} `json:"requestParameters"`
	ResponseElements  map[string]interface{} `json:"responseElements"`
	S3                S3Entity               `json:"s3"`
}

type S3Entity struct {
	Bucket           S3Bucket `json:"bucket"`
	ConfigurationId  string   `json:"configurationId"`
	Object           S3Object `json:"object"`
	S3SchemaVersion  string   `json:"s3SchemaVersion"`
}

type S3Bucket struct {
	Arn           string                 `json:"arn"`
	Name          string                 `json:"name"`
	OwnerIdentity map[string]interface{} `json:"ownerIdentity"`
}

type S3Object struct {
	ETag      string `json:"eTag"`
	Key       string `json:"key"`
	Sequencer string `json:"sequencer"`
	Size      int64  `json:"size"`
}

func main() {
	// AWS configuration
	ctx := context.Background()
	cfg, err := config.LoadDefaultConfig(ctx, config.WithRegion(AwsRegion))
	if err != nil {
		log.Fatalf("Failed to load AWS SDK configuration: %v", err)
	}

	s3Client := s3.NewFromConfig(cfg)
	sqsClient := sqs.NewFromConfig(cfg)
	stsClient := sts.NewFromConfig(cfg)

	output, err := stsClient.GetCallerIdentity(ctx, &sts.GetCallerIdentityInput{})
	if err != nil {
		log.Fatalf("Failed to get caller identity: %v", err)
	}
	accountId := *output.Account
	log.Printf("AWS Account ID: %s", accountId)

	bucketName := os.Getenv("S3_BUCKET_NAME")
	prefix := os.Getenv("S3_PREFIX")
	sqsQueueURL := os.Getenv("SQS_QUEUE_URL")

	if bucketName == "" || prefix == "" || sqsQueueURL == "" {
		log.Fatalf("S3_BUCKET_NAME, S3_PREFIX, and SQS_QUEUE_URL environment variables must be set")
	}

	log.Printf("Checking the number of objects in s3://%s/%s...", bucketName, prefix)
	objectCount, err := countObjects(ctx, s3Client, bucketName, prefix)
	if err != nil {
		log.Fatalf("Failed to count objects: %v", err)
	}
	fmt.Printf("Found %d objects in s3://%s/%s\n", objectCount, bucketName, prefix)

	if objectCount == 0 {
		fmt.Println("No objects found in the specified prefix. Exiting.")
		return
	}

	var confirmation string
	fmt.Printf("Do you want to send S3 PUT events for %d objects to SQS? (y/n): ", objectCount)
	fmt.Scanln(&confirmation)

	if confirmation != "y" && confirmation != "Y" {
		fmt.Println("Operation cancelled.")
		return
	}

	bar := progressbar.NewOptions64(
		int64(objectCount),
		progressbar.OptionSetDescription("Processing"),
		progressbar.OptionEnableColorCodes(true),
		progressbar.OptionShowBytes(false),
		progressbar.OptionSetWidth(40),
		progressbar.OptionShowCount(),
		progressbar.OptionSetTheme(progressbar.Theme{
			Saucer:        "[green]=[reset]",
			SaucerHead:    "[green]>[reset]",
			SaucerPadding: " ",
			BarStart:      "[",
			BarEnd:        "]",
		}),
		progressbar.OptionOnCompletion(func() { fmt.Println() }),
	)

	sem := semaphore.NewWeighted(int64(MaxConcurrency))
	var wg sync.WaitGroup
	var messagesSent atomic.Int64
	var failedMessages atomic.Int64
	startTime := time.Now()

	// Create event template - common part for all objects
	eventTemplate := S3EventRecord{
		AwsRegion:    AwsRegion,
		EventName:    "ObjectCreated:Put",
		EventSource:  "aws:s3",
		EventVersion: "2.1",
		RequestParameters: map[string]interface{}{
			"sourceIPAddress": "N/A",
		},
		ResponseElements: map[string]interface{}{
			"x-amz-id-2":       "SIMULATED_ID_2",
			"x-amz-request-id": "SIMULATED_REQUEST_ID",
		},
		S3: S3Entity{
			Bucket: S3Bucket{
				Arn:  fmt.Sprintf("arn:aws:s3:::%s", bucketName),
				Name: bucketName,
				OwnerIdentity: map[string]interface{}{
					"principalId": accountId,
				},
			},
			ConfigurationId: "SimulatedEvent",
			S3SchemaVersion: "1.0",
		},
	}

	var currentBatch []s3types.Object
	processObjectBatch := func(batch []s3types.Object) {
		defer wg.Done()
		defer sem.Release(1)

		for _, obj := range batch {
			objectKey := ""
			if obj.Key != nil {
				objectKey = *obj.Key
			}
			eTag := ""
			if obj.ETag != nil {
				eTag = *obj.ETag
			}

			record := eventTemplate
			record.EventTime = time.Now().UTC().Format(time.RFC3339Nano)
			record.S3.Object = S3Object{
				ETag:      eTag,
				Key:       objectKey,
				Size:      *obj.Size,
				Sequencer: fmt.Sprintf("%016X", time.Now().UnixNano()),
			}

			event := S3Event{
				Records: []S3EventRecord{record},
			}

			messageBody, err := json.Marshal(event)
			if err != nil {
				log.Printf("Failed to marshal message (object: %s): %v", objectKey, err)
				failedMessages.Add(1)
				_ = bar.Add(1)
				continue
			}

			_, err = sqsClient.SendMessage(ctx, &sqs.SendMessageInput{
				QueueUrl:    aws.String(sqsQueueURL),
				MessageBody: aws.String(string(messageBody)),
			})

			if err != nil {
				log.Printf("Failed to send message (object: %s): %v", objectKey, err)
				failedMessages.Add(1)
				_ = bar.Add(1)
				continue
			}
			
			messagesSent.Add(1)
			_ = bar.Add(1)
		}
	}

	err = listObjectsAndProcess(ctx, s3Client, bucketName, prefix, func(s3Object s3types.Object) {
		currentBatch = append(currentBatch, s3Object)
		
		if len(currentBatch) >= BatchSize {
			batchCopy := make([]s3types.Object, len(currentBatch))
			copy(batchCopy, currentBatch)
			currentBatch = nil
			
			wg.Add(1)
			if err := sem.Acquire(ctx, 1); err != nil {
				log.Printf("Failed to acquire semaphore: %v", err)
				wg.Done()
				return
			}
			
			go processObjectBatch(batchCopy)
		}
	})

	if len(currentBatch) > 0 {
		wg.Add(1)
		if err := sem.Acquire(ctx, 1); err != nil {
			log.Printf("Failed to acquire semaphore: %v", err)
		} else {
			go processObjectBatch(currentBatch)
		}
	}

	if err != nil {
		log.Fatalf("Error occurred while listing objects: %v", err)
	}

	wg.Wait()
	
	// Summary output
	duration := time.Since(startTime)
	fmt.Printf("\n=== Operation completed ===\n")
	fmt.Printf("- Processing time: %s\n", duration.Round(time.Millisecond))
	fmt.Printf("- Messages sent to SQS: %d\n", messagesSent.Load())
	if failedMessages.Load() > 0 {
		fmt.Printf("- Failed messages: %d\n", failedMessages.Load())
	}
	fmt.Printf("- Processing rate: %.2f objects/sec\n", float64(messagesSent.Load())/duration.Seconds())
}

// Helper function to count objects in S3 prefix
func countObjects(ctx context.Context, s3Client *s3.Client, bucketName string, prefix string) (int, error) {
	var count int
	paginator := s3.NewListObjectsV2Paginator(s3Client, &s3.ListObjectsV2Input{
		Bucket: aws.String(bucketName),
		Prefix: aws.String(prefix),
	})

	for paginator.HasMorePages() {
		page, err := paginator.NextPage(ctx)
		if err != nil {
			return 0, fmt.Errorf("Failed to get page for object counting: %w", err)
		}
		count += len(page.Contents)
	}
	return count, nil
}

// Helper function to list objects and process them
func listObjectsAndProcess(ctx context.Context, s3Client *s3.Client, bucketName string, prefix string, processObject func(s3types.Object)) error {
	paginator := s3.NewListObjectsV2Paginator(s3Client, &s3.ListObjectsV2Input{
		Bucket: aws.String(bucketName),
		Prefix: aws.String(prefix),
	})

	for paginator.HasMorePages() {
		page, err := paginator.NextPage(ctx)
		if err != nil {
			return fmt.Errorf("Failed to get page for object listing: %w", err)
		}
		for _, object := range page.Contents {
			processObject(object)
		}
	}
	return nil
}
