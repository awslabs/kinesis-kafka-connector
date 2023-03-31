package com.amazon.kinesis.kafka;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import com.amazonaws.util.StringUtils;
import com.google.common.util.concurrent.MoreExecutors;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.errors.RetriableException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;
import org.apache.kafka.connect.sink.SinkTaskContext;

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

	private String roleARN;

	private String roleExternalID;

	private String roleSessionName;

	private int roleDurationSeconds;

	private String kinesisEndpoint;

	private int maxConnections;

	private int rateLimit;

	private int maxBufferedTime;

	private int ttl;

	private String metricsLevel;

	private String metricsGranuality;

	private String metricsNameSpace;

	private boolean aggregation;

	private boolean usePartitionAsHashKey;

	private boolean flushSync;

	private boolean singleKinesisProducerPerPartition;

	private boolean pauseConsumption;

	private int outstandingRecordsThreshold;

	private int sleepPeriod;

	private int sleepCycles;

	private SinkTaskContext sinkTaskContext;

	private Map<String, KinesisProducer> producerMap = new HashMap<String, KinesisProducer>();

	private KinesisProducer kinesisProducer;

	private ConnectException putException;

	final FutureCallback<UserRecordResult> callback = new FutureCallback<UserRecordResult>() {
		@Override
		public void onFailure(Throwable t) {
			if (t instanceof UserRecordFailedException) {
				Attempt last = Iterables.getLast(((UserRecordFailedException) t).getResult().getAttempts());
				putException =  new RetriableException("Kinesis Producer was not able to publish data - " + last.getErrorCode() + "-"
						+ last.getErrorMessage());
				return;
			}
			putException = new RetriableException("Exception during Kinesis put", t);
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
		checkForEarlierPutException();

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
		checkForEarlierPutException();

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

			Futures.addCallback(f, callback, MoreExecutors.directExecutor());

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
							sinkTaskContext.pause((TopicPartition[]) sinkTaskContext.assignment().toArray(new TopicPartition[sinkTaskContext.assignment().size()]));
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
						sinkTaskContext.resume((TopicPartition[]) sinkTaskContext.assignment().toArray(new TopicPartition[sinkTaskContext.assignment().size()]));
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
						sinkTaskContext.pause((TopicPartition[]) sinkTaskContext.assignment().toArray(new TopicPartition[sinkTaskContext.assignment().size()]));
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
					sinkTaskContext.resume((TopicPartition[]) sinkTaskContext.assignment().toArray(new TopicPartition[sinkTaskContext.assignment().size()]));
				return true;
			}
		} else {
			return true;
		}
	}

	/**
	 * Examine whether an exception was reported from an earlier call to <code>put</code>.
	 * If so, then clear the exception and surface it up to Kafka Connect.
	 */
	private void checkForEarlierPutException() {
		if (putException != null) {
			final ConnectException e = putException;
			putException = null;
			throw e;
		}
	}

	private ListenableFuture<UserRecordResult> addUserRecord(KinesisProducer kp, String streamName, String partitionKey,
			boolean usePartitionAsHashKey, SinkRecord sinkRecord) {

		// If configured use kafka partition key as explicit hash key
		// This will be useful when sending data from same partition into
		// same shard
		if (usePartitionAsHashKey)
			return kp.addUserRecord(streamName, partitionKey, Integer.toString(sinkRecord.kafkaPartition()),
					DataUtility.parseValue(sinkRecord.valueSchema(), sinkRecord.value()));
		else
			return kp.addUserRecord(streamName, partitionKey,
					DataUtility.parseValue(sinkRecord.valueSchema(), sinkRecord.value()));

	}

	@Override
	public void start(Map<String, String> props) {

		streamName = props.get(AmazonKinesisSinkConnector.STREAM_NAME);

		maxConnections = Integer.parseInt(props.get(AmazonKinesisSinkConnector.MAX_CONNECTIONS));

		rateLimit = Integer.parseInt(props.get(AmazonKinesisSinkConnector.RATE_LIMIT));

		maxBufferedTime = Integer.parseInt(props.get(AmazonKinesisSinkConnector.MAX_BUFFERED_TIME));

		ttl = Integer.parseInt(props.get(AmazonKinesisSinkConnector.RECORD_TTL));

		regionName = props.get(AmazonKinesisSinkConnector.REGION);

		roleARN = props.get(AmazonKinesisSinkConnector.ROLE_ARN);

		roleSessionName = props.get(AmazonKinesisSinkConnector.ROLE_SESSION_NAME);

		roleDurationSeconds =  Integer.parseInt(props.get(AmazonKinesisSinkConnector.ROLE_DURATION_SECONDS));

		roleExternalID = props.get(AmazonKinesisSinkConnector.ROLE_EXTERNAL_ID);

		kinesisEndpoint = props.get(AmazonKinesisSinkConnector.KINESIS_ENDPOINT);

		metricsLevel = props.get(AmazonKinesisSinkConnector.METRICS_LEVEL);

		metricsGranuality = props.get(AmazonKinesisSinkConnector.METRICS_GRANUALITY);

		metricsNameSpace = props.get(AmazonKinesisSinkConnector.METRICS_NAMESPACE);

		aggregation = Boolean.parseBoolean(props.get(AmazonKinesisSinkConnector.AGGREGATION_ENABLED));

		usePartitionAsHashKey = Boolean.parseBoolean(props.get(AmazonKinesisSinkConnector.USE_PARTITION_AS_HASH_KEY));

		flushSync = Boolean.parseBoolean(props.get(AmazonKinesisSinkConnector.FLUSH_SYNC));

		singleKinesisProducerPerPartition = Boolean
				.parseBoolean(props.get(AmazonKinesisSinkConnector.SINGLE_KINESIS_PRODUCER_PER_PARTITION));

		pauseConsumption = Boolean.parseBoolean(props.get(AmazonKinesisSinkConnector.PAUSE_CONSUMPTION));

		outstandingRecordsThreshold = Integer
				.parseInt(props.get(AmazonKinesisSinkConnector.OUTSTANDING_RECORDS_THRESHOLD));

		sleepPeriod = Integer.parseInt(props.get(AmazonKinesisSinkConnector.SLEEP_PERIOD));

		sleepCycles = Integer.parseInt(props.get(AmazonKinesisSinkConnector.SLEEP_CYCLES));

		if (!singleKinesisProducerPerPartition)
			kinesisProducer = getKinesisProducer();

		putException = null;

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
				String producerName = topicPartition.partition() + "@" + topicPartition.topic();
				if (producerMap.containsKey(producerName)) {
					producerMap.get(producerName).destroy();
					producerMap.remove(producerName);
				}
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
			kinesisProducer.flushSync();
			kinesisProducer.destroy();
		}

	}

	private KinesisProducer getKinesisProducer() {
		KinesisProducerConfiguration config = new KinesisProducerConfiguration();
		config.setRegion(regionName);
		config.setCredentialsProvider(IAMUtility.createCredentials(regionName, roleARN, roleExternalID, roleSessionName, roleDurationSeconds));
		config.setMaxConnections(maxConnections);
		if (!StringUtils.isNullOrEmpty(kinesisEndpoint))
			config.setKinesisEndpoint(kinesisEndpoint);

		config.setAggregationEnabled(aggregation);

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
