### Introduction

The Kafka-Kinesis-Connector is a connector to be used with [Kafka Connect](https://kafka.apache.org/documentation/#connect) to publish messages from Kafka to [Amazon Kinesis Streams](https://aws.amazon.com/kinesis/streams/) or [Amazon Kinesis Firehose](https://aws.amazon.com/kinesis/firehose/).

Kafka-Kinesis-Connector for Firehose is used to publish messages from Kafka to one of the following destinations: [Amazon S3](https://aws.amazon.com/s3/), [Amazon Redshift](https://aws.amazon.com/redshift/), or [Amazon Elasticsearch Service](https://aws.amazon.com/elasticsearch-service/) and in turn enabling near real time analytics with existing business intelligence tools and dashboards. Amazon Kinesis Firehose has ability to transform, batch, archive message onto S3 and retry if destination is unavailable.

Kafka-Kinesis-Connector for Kinesis is used to publish messages from Kafka to Amazon Kinesis Streams.

Kafka-Kinesis-Connector can be executed on on-premise nodes or EC2 machines. It can be executed in standalone mode as well as [distributed mode](https://kafka.apache.org/documentation/#connect). 

### Building

You can build the project by running "maven package" and it will build amazon-kinesis-kafka-connector-X.X.X.jar

### Pre-Running Steps for Kafka-Kinesis-Connector for Firehose

1.  Make sure you create a delivery stream in AWS Console/CLI/SDK – See more details [here](http://docs.aws.amazon.com/firehose/latest/dev/basic-create.html) and configure destination.

2.  Connector uses [DefaultAWSCredentialsProviderChain](http://docs.aws.amazon.com/AWSJavaSDK/latest/javadoc/com/amazonaws/auth/DefaultAWSCredentialsProviderChain.html) for authenitication. It looks for credentials in following order - environment variable, java system properties, credentials profile file at default location ( (~/.aws/credentials), credentials delievered through Amazon EC2 container service, and instance profile credentails delivered through Amazon EC2 metadata service. Make sure user has at least permission to list streams/delivery stream, describe streams/delivery stream and put records for stream/delivery stream.

### Running a Connector

1.  Set up connector properties as shown in kafka-kinesis-firehose-connector.properties

2.  Set up worker properties for distributed/standalone mode and as per your requirements as shown in worker.properties or [here](https://kafka.apache.org/documentation/#connect_configuring)

3.  Copy artifact amazon-kinesis-kafka-connector-0.0.X.jar and export classpath or it can be added to JAVA_HOME/lib/ext

4.  You can use [REST API](https://kafka.apache.org/documentation/#connect_rest) to run Kafka Connect in distributed mode. For testing you can also run in standalone mode using following

`./bin/connect-standalone YOUR_PATH/worker.properties  YOUR_PATH/kinesis-kafka-firehose-connecter.properties`

#### kafka-kinesis-firehose-connector.properties


| Property        | Description          |  Value  |
| ------------- |:-------------:|:-----:|
| name     | User defined name of connector  | - |
| connector.class      | Class for Amazon Kinesis Firehose Connector      |   com.amazon.kinesis.kafka.FirehoseSinkConnector |
| topics | Kafka topics from where you want to consume messages. It can be single topic or comma separated list of topics      |   -  |
| region| Specify region of your Kinesis Firehose. Note, this parameter is mandatory, if kinesisEndpoint is provided | - |
| kinesisEndpoint| Alternate Kinesis endpoint, such as for a NAT gateway (optional) | - |
| roleARN | IAM Role ARN to assume (optional)| - |
| roleSessionName | IAM Role session-name to be logged (optional)| - |
| roleExternalID | IAM Role external-id (optional)| - |
| roleDurationSeconds | Duration of STS assumeRole session (optional)| - |
| batch | Connector batches messages before sending to Kinesis Firehose (true/false) | true |
| batchSize | Number of messages to be batched together. Firehose accepts at max 500 messages in one batch. | 500 |
| batchSizeInBytes | Message size in bytes when batched together. Firehose accepts at max 4MB in one batch. | 3670016 |
| deliveryStream | Firehose Destination Delivery Stream.| -


### Pre-Running Steps for Kafka-Kinesis-Connector for Streams

1.  Make sure you create Kinesis stream in AWS Console/CLI/SDK – See more details [here](http://docs.aws.amazon.com/streams/latest/dev/learning-kinesis-module-one-create-stream.html).

2.  Connector uses [DefaultAWSCredentialsProviderChain](http://docs.aws.amazon.com/AWSJavaSDK/latest/javadoc/com/amazonaws/auth/DefaultAWSCredentialsProviderChain.html) for authenitication. It looks for credentials in following order - environment variable, java system properties, credentials profile file at default location ( (~/.aws/credentials), credentials delievered through Amazon EC2 container service, and instance profile credentails delivered through Amazon EC2 metadata service. Make sure user has at least permission to list streams/delivery stream, describe streams/delivery stream and put records for stream/delivery stream.

### Running a Connector

1.  Set up connector properties as shown in kafka-kinesis-streams-connector.properties

2.  Set up worker properties for distributed/standalone mode and as per your requirements as shown in worker.properties or [here](https://kafka.apache.org/documentation/#connect_configuring)

3.  Copy artifact amazon-kinesis-kafka-connector-0.0.X.jar and export classpath or it can be added to JAVA_HOME/lib/ext

4.  You can use [REST API](https://kafka.apache.org/documentation/#connect_rest) to run Kafka Connect in distributed mode. For testing you can also run in standalone mode using following

`./bin/connect-standalone YOUR_PATH/worker.properties  YOUR_PATH/kinesis-kafka-streams-connecter.properties`

#### kafka-kinesis-streams-connector.properties


| Property        | Description          |  Value  |
| ------------- |:-------------:|:-----:|
| name     | User defined name of connector  | - |
| connector.class      | Class for Amazon Kinesis Stream Connector      |   com.amazon.kinesis.kafka.AmazonKinesisSinkConnector |
| topics | Kafka topics from where you want to consume messages. It can be single topic or comma separated list of topics      |   -  |
| region| Specify region of your Kinesis Data Streams | - |
| kinesisEndpoint| Alternate Kinesis endpoint, such as for a NAT gateway (optional) | - |
| streamName | Kinesis Stream Name.| - |
| roleARN | IAM Role ARN to assume (optional)| - |
| roleSessionName | IAM Role session-name to be logged (optional)| - |
| roleExternalID | IAM Role external-id (optional)| - |
| roleDurationSeconds | Duration of STS assumeRole session (optional)| - |
| usePartitionAsHashKey | Using Kafka partition key as hash key for Kinesis streams.  | false |
| maxBufferedTime | Maximum amount of time (milliseconds) a record may spend being buffered before it gets sent. Records may be sent sooner than this depending on the other buffering limits. Range: [100..... 9223372036854775807] | 15000 |
| maxConnections | Maximum number of connections to open to the backend. HTTP requests are sent in parallel over multiple connections. Range: [1...256]. | 24 |
| rateLimit | Limits the maximum allowed put rate for a shard, as a percentage of the backend limits. | 100 |
| ttl | Set a time-to-live on records (milliseconds). Records that do not get successfully put within the limit are failed. | 60000 |
| metricsLevel | Controls the number of metrics that are uploaded to CloudWatch. Expected pattern: none/summary/detailed | none |
| metricsGranuality | Controls the granularity of metrics that are uploaded to CloudWatch. Greater granularity produces more metrics. Expected pattern: global/stream/shard. |  global |
| metricsNameSpace | The namespace to upload metrics under. | KinesisProducer |
| aggregation | With aggregation, multiple user records are packed into a single KinesisRecord. If disabled, each user record is sent in its own KinesisRecord.| true
