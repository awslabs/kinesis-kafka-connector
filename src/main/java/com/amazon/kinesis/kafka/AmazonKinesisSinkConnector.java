package com.amazon.kinesis.kafka;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.sink.SinkConnector;

public class AmazonKinesisSinkConnector extends SinkConnector {

	public static final String REGION = "region";

	public static final String STREAM_NAME = "streamName";

	public static final String MAX_BUFFERED_TIME = "maxBufferedTime";

	public static final String MAX_CONNECTIONS = "maxConnections";

	public static final String RATE_LIMIT = "rateLimit";

	public static final String RECORD_TTL = "ttl";

	public static final String METRICS_LEVEL = "metricsLevel";

	public static final String METRICS_GRANUALITY = "metricsGranuality";

	public static final String METRICS_NAMESPACE = "metricsNameSpace";

	public static final String AGGREGRATION_ENABLED = "aggregration";

	public static final String USE_PARTITION_AS_HASH_KEY = "usePartitionAsHashKey";

	private String region;

	private String streamName;

	private String maxBufferedTime;

	private String maxConnections;

	private String rateLimit;

	private String ttl;

	private String metricsLevel;

	private String metricsGranuality;

	private String metricsNameSpace;

	private String aggregration;

	private String usePartitionAsHashKey;

	@Override
	public void start(Map<String, String> props) {
		region = props.get(REGION);
		streamName = props.get(STREAM_NAME);
		maxBufferedTime = props.get(MAX_BUFFERED_TIME);
		maxConnections = props.get(MAX_CONNECTIONS);
		rateLimit = props.get(RATE_LIMIT);
		ttl = props.get(RECORD_TTL);
		metricsLevel = props.get(METRICS_LEVEL);
		metricsGranuality = props.get(METRICS_GRANUALITY);
		metricsNameSpace = props.get(METRICS_NAMESPACE);
		aggregration = props.get(AGGREGRATION_ENABLED);
		usePartitionAsHashKey = props.get(USE_PARTITION_AS_HASH_KEY);
	}

	@Override
	public void stop() {
		// TODO Auto-generated method stub

	}

	@Override
	public Class<? extends Task> taskClass() {
		return AmazonKinesisSinkTask.class;
	}

	@Override
	public List<Map<String, String>> taskConfigs(int maxTasks) {
		ArrayList<Map<String, String>> configs = new ArrayList<>();
		for (int i = 0; i < maxTasks; i++) {
			Map<String, String> config = new HashMap<>();
			if (streamName != null)
				config.put(STREAM_NAME, streamName);

			if (region != null)
				config.put(REGION, region);

			if (maxBufferedTime != null)
				config.put(MAX_BUFFERED_TIME, maxBufferedTime);
			else
				// default value of 15000 ms
				config.put(MAX_BUFFERED_TIME, "15000");

			if (maxConnections != null)
				config.put(MAX_CONNECTIONS, maxConnections);
			else
				config.put(MAX_CONNECTIONS, "24");

			if (rateLimit != null)
				config.put(RATE_LIMIT, rateLimit);
			else
				config.put(RATE_LIMIT, "100");

			if (ttl != null)
				config.put(RECORD_TTL, ttl);
			else
				config.put(RECORD_TTL, "60000");

			if (metricsLevel != null)
				config.put(METRICS_LEVEL, metricsLevel);
			else
				config.put(METRICS_LEVEL, "none");

			if (metricsGranuality != null)
				config.put(METRICS_GRANUALITY, metricsGranuality);
			else
				config.put(METRICS_GRANUALITY, "global");

			if (metricsNameSpace != null)
				config.put(METRICS_NAMESPACE, metricsNameSpace);
			else
				config.put(METRICS_NAMESPACE, "KinesisProducer");

			if (aggregration != null)
				config.put(AGGREGRATION_ENABLED, aggregration);
			else
				config.put(AGGREGRATION_ENABLED, "false");

			if (usePartitionAsHashKey != null)
				config.put(USE_PARTITION_AS_HASH_KEY, usePartitionAsHashKey);
			else
				config.put(USE_PARTITION_AS_HASH_KEY, "false");

			configs.add(config);

		}
		return configs;
	}

	@Override
	public String version() {
		// TODO Auto-generated method stub
		return null;
	}

}
