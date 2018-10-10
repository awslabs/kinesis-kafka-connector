package com.amazon.kinesis.kafka;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;

import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;
import org.testng.annotations.Test;

public class BackendSinkRecordTransformationTest {
    private static final Schema KEY_SCHEMA = SchemaBuilder.struct().field("id", Schema.INT64_SCHEMA).build();
    private static final Schema VALUE_SCHEMA = SchemaBuilder.struct()
        .field("test1", Schema.STRING_SCHEMA)
        .field("test2", Schema.STRING_SCHEMA)
        .build();

    private final BackendSinkRecordTransformation subject = new BackendSinkRecordTransformation();

    @Test
    public void test() {
        final Struct key = new Struct(KEY_SCHEMA);
        key.put("id", 42L);
        final Struct value = new Struct(VALUE_SCHEMA);
        value.put("test1", "TEST");
        value.put("test2", "TSET");
        final SinkRecord record = new SinkRecord("UNUSED", 0, KEY_SCHEMA, key, VALUE_SCHEMA, value, 0);

        final SinkRecord actual = subject.apply(record);
        final Struct actualValue = (Struct) actual.value();
        final Struct actualValueData = actualValue.getStruct("data");

        for (Field field : VALUE_SCHEMA.fields()) {
            assertEquals(actualValueData.get(field.name()), value.get(field.name()));
        }

        assertNotNull(actualValue.getString("type"));
        assertNotNull(actualValue.getString("timestamp"));
        assertNotNull(actualValue.getStruct("context"));
    }
}
