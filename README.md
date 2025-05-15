# S3 PUT Event Generator

## Overview

This tool generates simulated S3 PUT events for existing objects in a specified S3 prefix and sends these events to an SQS queue. It's designed to process large numbers of objects efficiently using concurrency, with visual progress tracking.

## Features

- **High-Performance Processing**: Utilizes Go's concurrency for maximum throughput
- **Visual Progress Tracking**: Real-time progress bar using schollz/progressbar
- **Batch Processing**: Efficiently processes S3 objects in configurable batches
- **User Confirmation**: Shows object count before proceeding
- **Detailed Statistics**: Reports processing rate, success/failure counts, and total duration

## Requirements

- Go 1.18+
- AWS credentials configured (via environment variables, AWS CLI, or IAM role)
- Permissions:
  - S3: `s3:ListBucket` for the specified bucket
  - SQS: `sqs:SendMessage` for the target queue
  - STS: `sts:GetCallerIdentity` to obtain the AWS account ID

## Installation

1. Clone the repository:
```bash
git clone https://github.com/yourusername/s3-put-event-generator.git
cd s3-put-event-generator
```
2. Install dependencies:
```
go mod tidy
```

## Configuration

The tool is configured via environment variables:

| Variable | Description | Required |
|----------|-------------|:--------:|
| `S3_BUCKET_NAME` | Source S3 bucket name | ✅ |
| `S3_PREFIX` | Prefix path for objects in the bucket | ✅ |
| `SQS_QUEUE_URL` | Target SQS queue URL | ✅ |
| `AWS_REGION` | AWS region (defaults to ap-northeast-1 if not set) | ❌ |
| `AWS_PROFILE` | AWS profile to use (optional) | ❌ |

You can also modify the constants in the code to adjust:
- `MaxConcurrency`: Number of concurrent goroutines (default: 100)
- `AwsRegion`: Default AWS region if not specified by environment variable
- `BatchSize`: Number of objects to process in each batch (default: 10)

## Usage

1. Set the required environment variables:
```bash
export S3_BUCKET_NAME="your-bucket-name"
export S3_PREFIX="path/to/objects/"
export SQS_QUEUE_URL="https://sqs.region.amazonaws.com/account-id/queue-name"
```

2. Run the program:
```
go run main.go
```

3. Review the object count and confirm the operation

## Example

```bash
$ export S3_BUCKET_NAME="data-bucket"
$ export S3_PREFIX="logs/2023/"
$ export SQS_QUEUE_URL="https://sqs.ap-northeast-1.amazonaws.com/123456789012/event-processing-queue"
$ go run main.go

AWS Account ID: 123456789012
Counting objects in s3://data-bucket/logs/2023/...
Found 5432 objects in s3://data-bucket/logs/2023/
Do you want to proceed with sending S3 PUT events for 5432 objects to SQS? (y/n): y

Processing [==========>        ] 50.12% (2723/5432)

=== Operation Complete ===

Processing time: 45.213s

Messages sent to SQS: 5432

Processing rate: 120.14 objects/second
```

## How It Works

1. **Object Counting**: First, the tool counts all objects under the specified prefix to provide information to the user
2. **User Confirmation**: The user is asked to confirm before proceeding
3. **Event Generation**: For each object, the tool:
   - Creates an S3 event structure that simulates a PUT event
   - Includes the object's key, ETag, and size
   - Uses the current time as the event timestamp
4. **SQS Delivery**: The events are sent to the specified SQS queue
5. **Progress Tracking**: A progress bar shows completion percentage and processing statistics

## Performance Considerations

- Increase `MaxConcurrency` for higher throughput (if network and SQS throughput allow)
- Adjust `BatchSize` to balance memory usage and goroutine creation overhead
- AWS rate limits may apply to SQS and should be considered for very large object sets

## Build

To build a standalone executable:

```bash
go build -o s3-put-event-generator
```

## Contribution

Contributions are welcome! Please feel free to submit pull requests.
