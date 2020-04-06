package com.amazon.kinesis.kafka;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.utils.AppInfoParser;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.sink.SinkConnector;

public class FirehoseSinkConnector extends SinkConnector {

	public static final String DELIVERY_STREAM = "deliveryStream";
	
	public static final String REGION = "region";
	
	public static final String BATCH = "batch";
	
	public static final String BATCH_SIZE = "batchSize";
	
	public static final String BATCH_SIZE_IN_BYTES = "batchSizeInBytes";

	public static final String ROLE_ARN = "roleARN";

	public static final String ROLE_SESSION_NAME = "roleSessionName";

	public static final String ROLE_EXTERNAL_ID = "roleExternalID";

	public static final String ROLE_DURATION_SECONDS = "roleDurationSeconds";

	public static final String KINESIS_ENDPOINT = "kinesisEndpoint";

	private String deliveryStream;
	
	private String region;
	
	private String batch;
	
	private String batchSize;
	
	private String batchSizeInBytes;

	private String roleARN;

	private String roleSessionName;

	private String roleExternalID;

	private String roleDurationSeconds;

	private String kinesisEndpoint;

	private final String MAX_BATCH_SIZE = "500";
	
	private final String MAX_BATCH_SIZE_IN_BYTES = "3670016";

	@Override
	public void start(Map<String, String> props) {
		
		deliveryStream = props.get(DELIVERY_STREAM);
		region = props.get(REGION);
		batch = props.get(BATCH);	
		batchSize = props.get(BATCH_SIZE);
		batchSizeInBytes = props.get(BATCH_SIZE_IN_BYTES);
		roleARN = props.get(ROLE_ARN);
		roleSessionName = props.get(ROLE_SESSION_NAME);
		roleExternalID = props.get(ROLE_EXTERNAL_ID);
		roleDurationSeconds = props.get(ROLE_DURATION_SECONDS);
		kinesisEndpoint = props.get(KINESIS_ENDPOINT);
	}

	@Override
	public void stop() {
		// TODO Auto-generated method stub

	}

	@Override
	public Class<? extends Task> taskClass() {
		return FirehoseSinkTask.class;
	}

	@Override
	public List<Map<String, String>> taskConfigs(int maxTasks) {
		ArrayList<Map<String, String>> configs = new ArrayList<>();
		for (int i = 0; i < maxTasks; i++) {
			Map<String, String> config = new HashMap<>();
			if (deliveryStream != null)
				config.put(DELIVERY_STREAM, deliveryStream);
			
			if(region != null)
				config.put(REGION, region);
			
			if(batch != null)
				config.put(BATCH, batch);
			
			if(batchSize != null)
				config.put(BATCH_SIZE, batchSize);
			else
				config.put(BATCH_SIZE, MAX_BATCH_SIZE);
			
			if(batchSizeInBytes != null)
				config.put(BATCH_SIZE_IN_BYTES,  batchSizeInBytes);
			else 
				config.put(BATCH_SIZE_IN_BYTES, MAX_BATCH_SIZE_IN_BYTES);

			if (roleARN != null)
				config.put(ROLE_ARN, roleARN);

			if (roleSessionName != null)
				config.put(ROLE_SESSION_NAME, roleSessionName);

			if (roleExternalID != null)
				config.put(ROLE_EXTERNAL_ID, roleExternalID);

			if (roleDurationSeconds != null)
				config.put(ROLE_DURATION_SECONDS, roleDurationSeconds);
			else
				config.put(ROLE_DURATION_SECONDS, "3600");

			if (kinesisEndpoint != null)
				config.put(KINESIS_ENDPOINT, kinesisEndpoint);

			configs.add(config);
		}
		return configs;
	}

	@Override
	public String version() {
		// Currently using Kafka version, in future release use Kinesis-Kafka version
		return AppInfoParser.getVersion();
	}

	@Override
	public ConfigDef config() {
		return new ConfigDef();
	}

}
