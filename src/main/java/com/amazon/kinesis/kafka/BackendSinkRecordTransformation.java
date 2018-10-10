package com.amazon.kinesis.kafka;

import java.time.Clock;
import java.time.Instant;
import java.time.format.DateTimeFormatter;
import java.util.Map;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.transforms.Transformation;

public class BackendSinkRecordTransformation implements Transformation<SinkRecord> {

    @Override
    public SinkRecord apply(final SinkRecord in) {
        final Schema schema = SchemaBuilder.struct()
            .field("type", SchemaBuilder.string().build())
            .field("context", SchemaBuilder.struct().build())
            .field("timestamp", SchemaBuilder.string().build())
            .field("data", in.valueSchema())
            .build();

        final Struct value = new Struct(schema);
        value.put("type", "corepro_batch_transaction");
        value.put("context", new Struct(SchemaBuilder.struct().build()));
        value.put("timestamp", DateTimeFormatter.ISO_INSTANT.format(Instant.now(Clock.systemUTC())));
        value.put("data", in.value());

        return in.newRecord(in.topic(), in.kafkaPartition(), in.keySchema(), in.key(), schema, value, in.timestamp());
    }

    @Override
    public ConfigDef config() {
        return new ConfigDef();
    }

    @Override
    public void close() {
    }

    @Override
    public void configure(final Map<String, ?> map) {
    }
}
