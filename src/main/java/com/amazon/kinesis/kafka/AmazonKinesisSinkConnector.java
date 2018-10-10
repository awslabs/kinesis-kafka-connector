package com.amazon.kinesis.kafka;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.utils.AppInfoParser;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.SinkConnector;

public class AmazonKinesisSinkConnector extends SinkConnector {
	private Map<String, String> props = Collections.emptyMap();

	@Override
	public void start(final Map<String, String> props) {
		try {
			new AmazonKinesisSinkConfig(props);
		}
		catch (Exception e) {
			throw new ConnectException("Failed to start due to configuration error", e);
		}

		this.props = props;
	}

	@Override
	public void stop() {
	}

	@Override
	public Class<? extends Task> taskClass() {
		return AmazonKinesisSinkTask.class;
	}

	@Override
	public List<Map<String, String>> taskConfigs(final int maxTasks) {
		if (props.isEmpty()) {
			throw new ConnectException("Task configs not initialized");
		}
		return IntStream.range(0, maxTasks)
			.mapToObj(i -> this.props)
			.collect(Collectors.toList());
	}

	@Override
	public String version() {
		// Currently using Kafka version, in future release use Kinesis-Kafka version
		return AppInfoParser.getVersion();

	}

	@Override
	public ConfigDef config() {
		return AmazonKinesisSinkConfig.DEFINITION;
	}
}
