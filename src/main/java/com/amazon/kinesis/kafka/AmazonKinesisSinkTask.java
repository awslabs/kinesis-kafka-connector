package com.amazon.kinesis.kafka;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;
import org.apache.kafka.connect.sink.SinkTaskContext;
import org.apache.kafka.connect.storage.Converter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.auth.STSAssumeRoleSessionCredentialsProvider;
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
	private static final Logger LOG = LoggerFactory.getLogger(DataUtility.class);

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
	private boolean flushSync;
	private boolean singleKinesisProducerPerPartition;
	private boolean pauseConsumption;
	private int outstandingRecordsThreshold;
	private int sleepPeriod;
	private int sleepCycles;
	private String producerRole;
	private String stsSessionName;
	private SinkTaskContext sinkTaskContext;
	private Map<String, KinesisProducer> producerMap = new HashMap<String, KinesisProducer>();
	private KinesisProducer kinesisProducer;
	private Converter converter;

	private static final FutureCallback<UserRecordResult> CALLBACK = new FutureCallback<UserRecordResult>() {
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
	public void initialize(SinkTaskContext context) {
		sinkTaskContext = context;
	}

	@Override
	public String version() {
		return null;
	}

	@Override
	public void flush(Map<TopicPartition, OffsetAndMetadata> arg0) {
		if (singleKinesisProducerPerPartition) {
			producerMap.values().forEach(producer -> {
				if (flushSync)
					producer.flushSync();
				else
					producer.flush();
			});
		} else {
			if (flushSync)
				kinesisProducer.flushSync();
			else
				kinesisProducer.flush();
		}
	}

	@Override
	public void put(Collection<SinkRecord> sinkRecords) {

		// If KinesisProducers cannot write to Kinesis Streams (because of
		// connectivity issues, access issues
		// or misconfigured shards we will pause consumption of messages till
		// backlog is cleared

		validateOutStandingRecords();

		String partitionKey;
		for (SinkRecord sinkRecord : sinkRecords) {
			ListenableFuture<UserRecordResult> f;
			// Kinesis does not allow empty partition key
			if (sinkRecord.key() != null && !sinkRecord.key().toString().trim().equals("")) {
				partitionKey = sinkRecord.key().toString().trim();
			} else {
				partitionKey = Integer.toString(sinkRecord.kafkaPartition());
			}

			if (singleKinesisProducerPerPartition)
				f = addUserRecord(producerMap.get(sinkRecord.kafkaPartition() + "@" + sinkRecord.topic()), streamName,
						partitionKey, usePartitionAsHashKey, sinkRecord);
			else
				f = addUserRecord(kinesisProducer, streamName, partitionKey, usePartitionAsHashKey, sinkRecord);

			Futures.addCallback(f, CALLBACK);
		}
	}

	private boolean validateOutStandingRecords() {
		if (pauseConsumption) {
			if (singleKinesisProducerPerPartition) {
				producerMap.values().forEach(producer -> {
					int sleepCount = 0;
					boolean pause = false;
					// Validate if producer has outstanding records within
					// threshold values
					// and if not pause further consumption
					while (producer.getOutstandingRecordsCount() > outstandingRecordsThreshold) {
						try {
							// Pausing further
							sinkTaskContext.pause((TopicPartition[]) sinkTaskContext.assignment().toArray());
							pause = true;
							Thread.sleep(sleepPeriod);
							if (sleepCount++ > sleepCycles) {
								// Dummy message - Replace with your code to
								// notify/log that Kinesis Producers have
								// buffered values
								// but are not being sent
								System.out.println(
										"Kafka Consumption has been stopped because Kinesis Producers has buffered messages above threshold");
								sleepCount = 0;
							}
						} catch (InterruptedException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						}
					}
					if (pause)
						sinkTaskContext.resume((TopicPartition[]) sinkTaskContext.assignment().toArray());
				});
				return true;
			} else {
				int sleepCount = 0;
				boolean pause = false;
				// Validate if producer has outstanding records within threshold
				// values
				// and if not pause further consumption
				while (kinesisProducer.getOutstandingRecordsCount() > outstandingRecordsThreshold) {
					try {
						// Pausing further
						sinkTaskContext.pause((TopicPartition[]) sinkTaskContext.assignment().toArray());
						pause = true;
						Thread.sleep(sleepPeriod);
						if (sleepCount++ > sleepCycles) {
							// Dummy message - Replace with your code to
							// notify/log that Kinesis Producers have buffered
							// values
							// but are not being sent
							System.out.println(
									"Kafka Consumption has been stopped because Kinesis Producers has buffered messages above threshold");
							sleepCount = 0;
						}
					} catch (InterruptedException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
				}
				if (pause)
					sinkTaskContext.resume((TopicPartition[]) sinkTaskContext.assignment().toArray());
				return true;
			}
		} else {
			return true;
		}
	}

	private ListenableFuture<UserRecordResult> addUserRecord(KinesisProducer kp, String streamName, String partitionKey,
			boolean usePartitionAsHashKey, SinkRecord sinkRecord) {

		ByteBuffer data = ByteBuffer.wrap(converter.fromConnectData(sinkRecord.topic(), sinkRecord.valueSchema(), sinkRecord.value()));

		// If configured use kafka partition key as explicit hash key
		// This will be useful when sending data from same partition into
		// same shard
		if (usePartitionAsHashKey)
			return kp.addUserRecord(streamName, partitionKey, Integer.toString(sinkRecord.kafkaPartition()), data);
		else
			return kp.addUserRecord(streamName, partitionKey, data);
	}

	@Override
	public void start(Map<String, String> props) {
    AmazonKinesisSinkConfig config = new AmazonKinesisSinkConfig(props);
		streamName = config.getString(AmazonKinesisSinkConfig.STREAM_NAME);
		maxConnections = config.getInt(AmazonKinesisSinkConfig.MAX_CONNECTIONS);
		rateLimit = config.getInt(AmazonKinesisSinkConfig.RATE_LIMIT);
		maxBufferedTime = config.getInt(AmazonKinesisSinkConfig.MAX_BUFFERED_TIME);
		ttl = config.getInt(AmazonKinesisSinkConfig.RECORD_TTL);
		regionName = config.getString(AmazonKinesisSinkConfig.REGION);
		metricsLevel = config.getString(AmazonKinesisSinkConfig.METRICS_LEVEL);
		metricsGranuality = config.getString(AmazonKinesisSinkConfig.METRICS_GRANULARITY);
		metricsNameSpace = config.getString(AmazonKinesisSinkConfig.METRICS_NAMESPACE);
		aggregration = config.getBoolean(AmazonKinesisSinkConfig.AGGREGRATION_ENABLED);
		usePartitionAsHashKey = config.getBoolean(AmazonKinesisSinkConfig.USE_PARTITION_AS_HASH_KEY);
		flushSync = config.getBoolean(AmazonKinesisSinkConfig.FLUSH_SYNC);
		singleKinesisProducerPerPartition = config.getBoolean(AmazonKinesisSinkConfig.SINGLE_KINESIS_PRODUCER_PER_PARTITION);
		pauseConsumption = config.getBoolean(AmazonKinesisSinkConfig.PAUSE_CONSUMPTION);
		outstandingRecordsThreshold = config.getInt(AmazonKinesisSinkConfig.OUTSTANDING_RECORDS_THRESHOLD);
		sleepPeriod = config.getInt(AmazonKinesisSinkConfig.SLEEP_PERIOD);
		sleepCycles = config.getInt(AmazonKinesisSinkConfig.SLEEP_CYCLES);
		producerRole = config.getString(AmazonKinesisSinkConfig.PRODUCER_ROLE);
		stsSessionName = config.getString(AmazonKinesisSinkConfig.STS_SESSION_NAME);
		converter = config.converter();

		if (!singleKinesisProducerPerPartition)
			kinesisProducer = getKinesisProducer();
	}

	public void open(Collection<TopicPartition> partitions) {
		if (singleKinesisProducerPerPartition) {
			for (TopicPartition topicPartition : partitions) {
				producerMap.put(topicPartition.partition() + "@" + topicPartition.topic(), getKinesisProducer());
			}
		}
	}

	public void close(Collection<TopicPartition> partitions) {
		if (singleKinesisProducerPerPartition) {
			for (TopicPartition topicPartition : partitions) {
				producerMap.get(topicPartition.partition() + "@" + topicPartition.topic()).destroy();
				producerMap.remove(topicPartition.partition() + "@" + topicPartition.topic());
			}
		}
	}

	@Override
	public void stop() {
		// destroying kinesis producers which were not closed as part of close
		if (singleKinesisProducerPerPartition) {
			for (KinesisProducer kp : producerMap.values()) {
				kp.flushSync();
				kp.destroy();
			}
		} else {
			kinesisProducer.destroy();
		}

	}

	private KinesisProducer getKinesisProducer() {
		KinesisProducerConfiguration config = new KinesisProducerConfiguration();
		config.setRegion(regionName);
		config.setCredentialsProvider(getCredentialsProvider());
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

	private AWSCredentialsProvider getCredentialsProvider() {
		if (producerRole == null) {
			return new DefaultAWSCredentialsProviderChain();
		}
		return new STSAssumeRoleSessionCredentialsProvider.Builder(producerRole, stsSessionName).build();
	}

}
