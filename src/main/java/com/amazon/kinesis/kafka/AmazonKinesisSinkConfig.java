package com.amazon.kinesis.kafka;

import java.util.Map;

import org.apache.kafka.common.Configurable;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigDef.Type;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.connect.storage.Converter;
import org.apache.kafka.connect.storage.ConverterType;

import com.google.common.collect.ImmutableMap;

public class AmazonKinesisSinkConfig extends AbstractConfig {
    public static final String REGION = "region";
    public static final String STREAM_NAME = "streamName";
    public static final String MAX_BUFFERED_TIME = "maxBufferedTime";
    public static final String MAX_CONNECTIONS = "maxConnections";
    public static final String RATE_LIMIT = "rateLimit";
    public static final String RECORD_TTL = "ttl";
    public static final String METRICS_LEVEL = "metricsLevel";
    public static final String METRICS_GRANULARITY = "metricsGranuality";
    public static final String METRICS_NAMESPACE = "metricsNameSpace";
    public static final String AGGREGRATION_ENABLED = "aggregration";
    public static final String USE_PARTITION_AS_HASH_KEY = "usePartitionAsHashKey";
    public static final String FLUSH_SYNC = "flushSync";
    public static final String SINGLE_KINESIS_PRODUCER_PER_PARTITION = "singleKinesisProducerPerPartition";
    public static final String PAUSE_CONSUMPTION = "pauseConsumption";
    public static final String OUTSTANDING_RECORDS_THRESHOLD = "outstandingRecordsThreshold";
    public static final String SLEEP_PERIOD = "sleepPeriod";
    public static final String SLEEP_CYCLES = "sleepCycles";
    public static final String PRODUCER_ROLE = "producerRole";
    public static final String STS_SESSION_NAME = "stsSessionName";
    public static final String SINK_RECORD_CONVERTER = "sinkRecordConverter";

    public static final ConfigDef DEFINITION = new ConfigDef();

    static {
        DEFINITION.define(REGION, Type.STRING, Importance.HIGH, "AWS Region");
        DEFINITION.define(STREAM_NAME, Type.STRING, Importance.HIGH, "Kinesis stream name");
        DEFINITION.define(MAX_BUFFERED_TIME, Type.INT, 15000, Importance.HIGH, "Max bufferd time");
        DEFINITION.define(MAX_CONNECTIONS, Type.INT, 24, Importance.MEDIUM, "Max conns");
        DEFINITION.define(RATE_LIMIT, Type.INT, 100, Importance.MEDIUM, "Rate limit");
        DEFINITION.define(RECORD_TTL, Type.INT, 60000, Importance.MEDIUM, "Record ttl");
        DEFINITION.define(METRICS_LEVEL, Type.STRING, "none", Importance.MEDIUM, "Metrics level");
        DEFINITION.define(METRICS_GRANULARITY, Type.STRING, "global", Importance.MEDIUM, "Metrics granularity");
        DEFINITION.define(METRICS_NAMESPACE, Type.STRING, "KinesisProducer", Importance.MEDIUM, "Metrics namespace");
        DEFINITION.define(AGGREGRATION_ENABLED, Type.BOOLEAN, false, Importance.MEDIUM, "Aggregation enabled");
        DEFINITION.define(USE_PARTITION_AS_HASH_KEY, Type.BOOLEAN, false, Importance.MEDIUM, "Use partition as hash key");
        DEFINITION.define(FLUSH_SYNC, Type.BOOLEAN, true, Importance.MEDIUM, "Flush sync");
        DEFINITION.define(SINGLE_KINESIS_PRODUCER_PER_PARTITION, Type.BOOLEAN, false, Importance.MEDIUM, "Single kinesis producer per partition");
        DEFINITION.define(PAUSE_CONSUMPTION, Type.BOOLEAN, true, Importance.MEDIUM, "Pause consumption");
        DEFINITION.define(OUTSTANDING_RECORDS_THRESHOLD, Type.INT, 500000, Importance.MEDIUM, "Outstanding records threshold");
        DEFINITION.define(SLEEP_PERIOD, Type.INT, 1000, Importance.MEDIUM, "Sleep period");
        DEFINITION.define(SLEEP_CYCLES, Type.INT, 10, Importance.MEDIUM, "Sleep cycles");
        DEFINITION.define(PRODUCER_ROLE, Type.STRING, Importance.HIGH, "Producer role");
        DEFINITION.define(STS_SESSION_NAME, Type.STRING, "AmazonKinesisSink", Importance.MEDIUM, "Sts session name");
        DEFINITION.define(SINK_RECORD_CONVERTER, Type.CLASS, Importance.MEDIUM, "Sink record converter type");
    }

    public AmazonKinesisSinkConfig(final Map<String, String> props) {
        super(AmazonKinesisSinkConfig.DEFINITION, props);
    }

    public <T> T getConfiguredInstance(String key, Class<T> t, Map<String, Object> configOverrides) {
        Class<?> c = this.getClass(key);
        if (c == null) {
            return null;
        }
        else {
            Object o = Utils.newInstance(c);
            if (!t.isInstance(o)) {
                throw new KafkaException(c.getName() + " is not an instance of " + t.getName());
            }
            else {
                if (o instanceof Configurable) {
                    Map<String, Object> configPairs = this.originals();
                    configPairs.putAll(configOverrides);
                    ((Configurable)o).configure(configPairs);
                }

                return t.cast(o);
            }
        }
    }

    public Converter converter() {
        return getConfiguredInstance(SINK_RECORD_CONVERTER, Converter.class,
            ImmutableMap.of(
                // TODO Make these configurable if needed
                "schemas.enable", false,
                "converter.type", ConverterType.VALUE.getName()
            )
        );
    }
}
