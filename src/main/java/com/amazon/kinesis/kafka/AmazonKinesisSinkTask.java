package com.amazon.kinesis.kafka;

import java.util.Collection;
import java.util.Map;
import java.util.Random;

import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;

import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.services.kinesis.producer.Attempt;
import com.amazonaws.services.kinesis.producer.KinesisProducer;
import com.amazonaws.services.kinesis.producer.KinesisProducerConfiguration;
import com.amazonaws.services.kinesis.producer.UserRecordFailedException;
import com.amazonaws.services.kinesis.producer.UserRecordResult;
import com.google.common.collect.Iterables;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;

public class AmazonKinesisSinkTask extends SinkTask {


	private String streamName;

	private String regionName;

	private int maxConnections;

	private int rateLimit;

	private int maxBufferedTime;

	private int ttl;

	private String metricsLevel;

	private String metricsGranuality;

	private String metricsNameSpace;

	private boolean aggregration;

	private boolean usePartitionAsHashKey;

	private KinesisProducer kinesisProducer;

	private int partitionKeysCount = 1_000_000;

	private Random random = new Random();
	

	final FutureCallback<UserRecordResult> callback = new FutureCallback<UserRecordResult>() {
		@Override
		public void onFailure(Throwable t) {
			if (t instanceof UserRecordFailedException) {
				Attempt last = Iterables.getLast(((UserRecordFailedException) t).getResult().getAttempts());
				throw new DataException("Kinesis Producer was not able to publish data - " + last.getErrorCode() + "-"
						+ last.getErrorMessage());
 
			}
			throw new DataException("Exception during Kinesis put", t);
		}

		@Override
		public void onSuccess(UserRecordResult result) {

		}
	};

	@Override
	public String version() {
		return null;
	}

	@Override
	public void flush(Map<TopicPartition, OffsetAndMetadata> arg0) {
		// TODO Auto-generated method stub
		// Setting to Synchronous Flush instead of Async
		// https://github.com/awslabs/kinesis-kafka-connector/issues/2
		kinesisProducer.flushSync();
	}

	@Override
	public void put(Collection<SinkRecord> sinkRecords) {
		String partitionKey;
		for (SinkRecord sinkRecord : sinkRecords) {
			ListenableFuture<UserRecordResult> f;
			// Kinesis does not allow empty partition key 
			if(sinkRecord.key()!= null && !sinkRecord.key().toString().trim().equals("")){
				partitionKey =  sinkRecord.key().toString().trim();
			}	
			else{
				partitionKey = Integer.toString(random.nextInt(partitionKeysCount));
			}
			// If configured use kafka partition key as explicit hash key
			// This will be useful when sending data from same partition into
			// same shard
			
			if (usePartitionAsHashKey)
				f = kinesisProducer.addUserRecord(streamName, partitionKey,
						Integer.toString(sinkRecord.kafkaPartition()),
						DataUtility.parseValue(sinkRecord.valueSchema(), sinkRecord.value()));
			else
				f = kinesisProducer.addUserRecord(streamName, partitionKey ,
						DataUtility.parseValue(sinkRecord.valueSchema(), sinkRecord.value()));

			Futures.addCallback(f, callback);

		}
	}

	@Override
	public void start(Map<String, String> props) {

		streamName = props.get(AmazonKinesisSinkConnector.STREAM_NAME);

		maxConnections = Integer.parseInt(props.get(AmazonKinesisSinkConnector.MAX_CONNECTIONS));

		rateLimit = Integer.parseInt(props.get(AmazonKinesisSinkConnector.RATE_LIMIT));

		maxBufferedTime = Integer.parseInt(props.get(AmazonKinesisSinkConnector.MAX_BUFFERED_TIME));

		ttl = Integer.parseInt(props.get(AmazonKinesisSinkConnector.RECORD_TTL));

		regionName = props.get(AmazonKinesisSinkConnector.REGION);

		metricsLevel = props.get(AmazonKinesisSinkConnector.METRICS_LEVEL);

		metricsGranuality = props.get(AmazonKinesisSinkConnector.METRICS_GRANUALITY);

		metricsNameSpace = props.get(AmazonKinesisSinkConnector.METRICS_NAMESPACE);

		aggregration = Boolean.parseBoolean(props.get(AmazonKinesisSinkConnector.AGGREGRATION_ENABLED));

		usePartitionAsHashKey = Boolean.parseBoolean(props.get(AmazonKinesisSinkConnector.USE_PARTITION_AS_HASH_KEY));

		
		kinesisProducer = getKinesisProducer();
				
	}

	@Override
	public void stop() {
		kinesisProducer.destroy();        

	}

	

	private KinesisProducer getKinesisProducer() {
		KinesisProducerConfiguration config = new KinesisProducerConfiguration();
		config.setRegion(regionName);
		config.setCredentialsProvider(new DefaultAWSCredentialsProviderChain());
		config.setMaxConnections(maxConnections);

		config.setAggregationEnabled(aggregration);

		// Limits the maximum allowed put rate for a shard, as a percentage of
		// the
		// backend limits.
		config.setRateLimit(rateLimit);

		// Maximum amount of time (milliseconds) a record may spend being
		// buffered
		// before it gets sent. Records may be sent sooner than this depending
		// on the
		// other buffering limits
		config.setRecordMaxBufferedTime(maxBufferedTime);

		// Set a time-to-live on records (milliseconds). Records that do not get
		// successfully put within the limit are failed.
		config.setRecordTtl(ttl);

		// Controls the number of metrics that are uploaded to CloudWatch.
		// Expected pattern: none|summary|detailed
		config.setMetricsLevel(metricsLevel);

		// Controls the granularity of metrics that are uploaded to CloudWatch.
		// Greater granularity produces more metrics.
		// Expected pattern: global|stream|shard
		config.setMetricsGranularity(metricsGranuality);

		// The namespace to upload metrics under.
		config.setMetricsNamespace(metricsNameSpace);

		return new KinesisProducer(config);

	}

}
