package com.amazon.kinesis.kafka;

import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.testng.annotations.Test;
import org.testng.Assert;

public class DataUtilityTest {
	
	@Test
	public void parseInt8ValueTest(){
	
		Schema schema = SchemaBuilder.int8();
		ByteBuffer actual = DataUtility.parseValue(schema, (byte) 2);
		ByteBuffer expected = ByteBuffer.allocate(1).put((byte) 2);
		
		Assert.assertTrue(actual.equals(expected));
		
	}
	
	@Test
	public void parseShortValueTest(){
		
		Schema schema = SchemaBuilder.int16();
		ByteBuffer actual = DataUtility.parseValue(schema, (short) 2);
		ByteBuffer expected = ByteBuffer.allocate(2).putShort((short) 2);
		
		Assert.assertTrue(actual.equals(expected));
	
	}
	
	@Test
	public void parseIntValueTest(){
		
		Schema schemaInt32 = SchemaBuilder.int32();
		ByteBuffer actualInt32 = DataUtility.parseValue(schemaInt32, (int) 2);
		ByteBuffer expectedInt32 = ByteBuffer.allocate(4).putInt( (int) 2);
		
		Assert.assertTrue(actualInt32.equals(expectedInt32));
	}
	
	@Test
	public void  parseLongValueTest(){
		
		Schema schema = SchemaBuilder.int64();
		ByteBuffer actual = DataUtility.parseValue(schema, (long) 2);
		ByteBuffer expected = ByteBuffer.allocate(8).putLong( (long) 2);
		
		Assert.assertTrue(actual.equals(expected));
		
	}
	
	@Test
	public void parseFloatValueTest(){
	
		Schema schema = SchemaBuilder.float32();
		ByteBuffer actual = DataUtility.parseValue(schema, (float) 2);
		ByteBuffer expected = ByteBuffer.allocate(4).putFloat((float) 2);
		
		Assert.assertTrue(actual.equals(expected));
		
	}
	
	@Test
	public void parseDoubleValueTest(){
	
		Schema schema = SchemaBuilder.float64();
		ByteBuffer actual = DataUtility.parseValue(schema, (double) 2);
		ByteBuffer expected = ByteBuffer.allocate(8).putDouble((double) 2);
		
		Assert.assertTrue(actual.equals(expected));
		
	}
	
	@Test
	public void parseBooleanValueTest(){
		
		Schema schema = SchemaBuilder.bool();
		ByteBuffer actual = DataUtility.parseValue(schema, (boolean) true);
		ByteBuffer expected = ByteBuffer.allocate(1).put( (byte) 1);
		
		Assert.assertTrue(actual.equals(expected));
	}
	
	@Test
	public void parseStringValueTest(){
		
		Schema schema = SchemaBuilder.string();
		ByteBuffer actual = DataUtility.parseValue(schema, "Testing Kinesis-Kafka Connector");
		ByteBuffer expected = null;
		try {
			expected = ByteBuffer.wrap(((String) "Testing Kinesis-Kafka Connector").getBytes("UTF-8"));
		} catch (UnsupportedEncodingException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		Assert.assertTrue(actual.equals(expected));

	}
	
	@Test
	public void parseArrayValueTest(){
		
		Schema valueSchema = SchemaBuilder.int8(); 
		Schema schema = SchemaBuilder.array(valueSchema);
		
		Object[] arrayOfInt8 = { (byte) 1, (byte) 2, (byte) 3, (byte) 4};
		ByteBuffer actual = DataUtility.parseValue(schema, arrayOfInt8);
		ByteBuffer expected = ByteBuffer.allocate(4).put((byte) 1).put( (byte) 2).put( (byte) 3).put( (byte) 4);
		
		Assert.assertTrue(actual.equals(expected));
		
	}
	
	@Test
	public void parseByteValueTest(){
		
		Schema schema = SchemaBuilder.bytes();
		
		byte[] value = "Kinesis-Kafka Connector".getBytes();
		ByteBuffer actual = DataUtility.parseValue(schema, value);
		ByteBuffer expected = ByteBuffer.wrap("Kinesis-Kafka Connector".getBytes());
		
		Assert.assertTrue(actual.equals(expected));
	}

}
