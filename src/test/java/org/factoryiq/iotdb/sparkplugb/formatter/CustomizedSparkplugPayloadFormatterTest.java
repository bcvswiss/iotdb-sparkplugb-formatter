package org.factoryiq.iotdb.sparkplugb.formatter;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.apache.iotdb.db.protocol.mqtt.Message;
import org.eclipse.tahu.message.SparkplugBPayloadEncoder;
import org.eclipse.tahu.message.model.Metric;
import org.eclipse.tahu.message.model.MetricDataType;
import org.eclipse.tahu.message.model.SparkplugBPayload;
import org.eclipse.tahu.message.model.PropertySet;
import org.eclipse.tahu.message.model.PropertyValue;
import org.eclipse.tahu.message.model.PropertyDataType;
import org.junit.Before;
import org.junit.Test;

import java.util.Date;
import java.util.List;
import java.util.UUID;
import java.util.ArrayList;
import java.math.BigInteger;

import static org.junit.Assert.*;

public class CustomizedSparkplugPayloadFormatterTest {

    private CustomizedSparkplugPayloadFormatter formatter;
    @Before
    public void setup() {
        formatter = new CustomizedSparkplugPayloadFormatter();
    }

    @Test
    public void testInvalidTopic() {
        ByteBuf payload = Unpooled.wrappedBuffer(new byte[]{1, 2, 3});
        List<Message> messages = formatter.format(payload);
        assertTrue("Invalid topic should return empty list", messages.isEmpty());
    }

    @Test
    public void testNullInputs() {
        assertTrue("Null payload should return empty list", 
                   formatter.format(null).isEmpty());
        assertTrue("Null topic should return empty list", 
                   formatter.format(Unpooled.wrappedBuffer(new byte[]{1})).isEmpty());
    }

    @Test
    public void testNullMetricValue() throws Exception {
        Date timestamp = new Date();
        List<Metric> metrics = new ArrayList<>();
        SparkplugBPayload payload = new SparkplugBPayload(
            timestamp,    // timestamp
            metrics,      // explicitly pass initialized metrics list
            0L,          // seq
            UUID.randomUUID().toString(), // uuid
            null         // body
        );
        
        Metric metric = new Metric.MetricBuilder("NullMetric", MetricDataType.String, null)
            .createMetric();
        payload.addMetric(metric);
        
        byte[] bytes = new SparkplugBPayloadEncoder().getBytes(payload, false);
        ByteBuf byteBuf = Unpooled.wrappedBuffer(bytes);

        List<Message> messages = formatter.format(byteBuf);
        assertNotNull(messages);
        assertEquals(1, messages.size());
        assertEquals("null", messages.get(0).getValues().get(0));
        assertEquals("null", messages.get(0).getMeasurements().get(0));
    }

    @Test
    public void testUnknownDataType() throws Exception {
        Date timestamp = new Date();
        List<Metric> metrics = new ArrayList<>();
        SparkplugBPayload payload = new SparkplugBPayload(
            timestamp,    // timestamp
            metrics,      // explicitly pass initialized metrics list
            0L,          // seq
            UUID.randomUUID().toString(), // uuid
            null         // body
        );
        
        Metric metric = new Metric.MetricBuilder("CustomMetric", MetricDataType.String, "custom value")
            .createMetric();
        payload.addMetric(metric);
        
        byte[] bytes = new SparkplugBPayloadEncoder().getBytes(payload, false);
        ByteBuf byteBuf = Unpooled.wrappedBuffer(bytes);

        List<Message> messages = formatter.format(byteBuf);
        assertNotNull(messages);
        assertEquals(1, messages.size());
        assertEquals("custom_value", messages.get(0).getValues().get(0));
        assertEquals("custom_metric", messages.get(0).getMeasurements().get(0));
    }

    @Test
    public void testBasicMetricValue() throws Exception {
        Date timestamp = new Date();
        List<Metric> metrics = new ArrayList<>();
        SparkplugBPayload payload = new SparkplugBPayload(
            timestamp,    // timestamp
            metrics,      // explicitly pass initialized metrics list
            0L,          // seq
            UUID.randomUUID().toString(), // uuid
            null         // body
        );
        
        Metric metric = new Metric.MetricBuilder("Temperature", MetricDataType.Float, 23.5f)
            .createMetric();
        payload.addMetric(metric);
        
        byte[] bytes = new SparkplugBPayloadEncoder().getBytes(payload, false);
        ByteBuf byteBuf = Unpooled.wrappedBuffer(bytes);

        List<Message> messages = formatter.format(byteBuf);
        assertNotNull(messages);
        assertEquals(1, messages.size());
        assertEquals("23.500000", messages.get(0).getValues().get(0));
        assertEquals("temperature", messages.get(0).getMeasurements().get(0));
    }

    @Test
    public void testModbusPayload() throws Exception {
        Date timestamp = new Date();
        List<Metric> metrics = new ArrayList<>();
        SparkplugBPayload payload = new SparkplugBPayload(
            timestamp, metrics, 0L, UUID.randomUUID().toString(), null
        );
        
        // Add Holding Registers metric with properties
        PropertySet properties1 = new PropertySet();
        properties1.put("source", new PropertyValue<>(PropertyDataType.String, "modbus"));
        properties1.put("source_type", new PropertyValue<>(PropertyDataType.String, "fc3"));
        properties1.put("status", new PropertyValue<>(PropertyDataType.String, "0x00000000"));
        
        Metric holdingRegister = new Metric.MetricBuilder("Holding Registers Block_0", MetricDataType.Double, 0.0)
            .properties(properties1)
            .createMetric();
        payload.addMetric(holdingRegister);
        
        // Add Coils metric
        Metric coils = new Metric.MetricBuilder("Coils Block_1", MetricDataType.Boolean, false)
            .createMetric();
        payload.addMetric(coils);
        
        byte[] bytes = new SparkplugBPayloadEncoder().getBytes(payload, false);
        ByteBuf byteBuf = Unpooled.wrappedBuffer(bytes);

        List<Message> messages = formatter.format(byteBuf);
        assertNotNull(messages);
        assertEquals(2, messages.size()); // One message per metric
        
        // First message (Holding Registers)
        assertEquals("0.000000", messages.get(0).getValues().get(0));
        assertEquals("holding_registers_block_0", messages.get(0).getMeasurements().get(0));
        
        // Second message (Coils)
        assertEquals("false", messages.get(1).getValues().get(0));
        assertEquals("coils_block_1", messages.get(1).getMeasurements().get(0));
    }

    @Test
    public void testOpcUaPayload() throws Exception {
        Date timestamp = new Date();
        List<Metric> metrics = new ArrayList<>();
        SparkplugBPayload payload = new SparkplugBPayload(
            timestamp, metrics, 0L, UUID.randomUUID().toString(), null
        );
        
        // Add DeviceHealth metric with properties
        PropertySet properties1 = new PropertySet();
        properties1.put("opcua_type", new PropertyValue<>(PropertyDataType.String, "TypeIDInt32"));
        properties1.put("opcua_status", new PropertyValue<>(PropertyDataType.String, "0x00000000"));
        
        Metric deviceHealth = new Metric.MetricBuilder("DeviceHealth", MetricDataType.Int32, 0)
            .properties(properties1)
            .createMetric();
        payload.addMetric(deviceHealth);
        
        // Add AnalogInput metric with properties
        PropertySet properties2 = new PropertySet();
        properties2.put("opcua_type", new PropertyValue<>(PropertyDataType.String, "TypeIDString"));
        properties2.put("opcua_status", new PropertyValue<>(PropertyDataType.String, "0x00000000"));
        
        Metric analogInput = new Metric.MetricBuilder("AnalogInput", MetricDataType.String, "1.0")
            .properties(properties2)
            .createMetric();
        payload.addMetric(analogInput);
        
        byte[] bytes = new SparkplugBPayloadEncoder().getBytes(payload, false);
        ByteBuf byteBuf = Unpooled.wrappedBuffer(bytes);

        List<Message> messages = formatter.format(byteBuf);
        assertNotNull(messages);
        assertEquals(2, messages.size()); // One message per metric
        
        // First message (DeviceHealth)
        assertEquals("0", messages.get(0).getValues().get(0));
        assertEquals("device_health", messages.get(0).getMeasurements().get(0));
        
        // Second message (AnalogInput)
        assertEquals("1.0", messages.get(1).getValues().get(0));
        assertEquals("analog_input", messages.get(1).getMeasurements().get(0));
    }

    @Test
    public void testMultipleMetrics() throws Exception {
        Date timestamp = new Date();
        List<Metric> metrics = new ArrayList<>();
        SparkplugBPayload payload = new SparkplugBPayload(
            timestamp, metrics, 0L, UUID.randomUUID().toString(), null
        );
        
        payload.addMetric(new Metric.MetricBuilder("Temperature", MetricDataType.Float, 23.5f).createMetric());
        payload.addMetric(new Metric.MetricBuilder("Humidity", MetricDataType.Int32, 45).createMetric());
        payload.addMetric(new Metric.MetricBuilder("Status", MetricDataType.Boolean, true).createMetric());
        
        byte[] bytes = new SparkplugBPayloadEncoder().getBytes(payload, false);
        ByteBuf byteBuf = Unpooled.wrappedBuffer(bytes);

        List<Message> messages = formatter.format(byteBuf);
        assertNotNull(messages);
        assertEquals(3, messages.size()); // One message per metric
        
        assertEquals("23.500000", messages.get(0).getValues().get(0));
        assertEquals("45", messages.get(1).getValues().get(0));
        assertEquals("true", messages.get(2).getValues().get(0));
        
        assertEquals("temperature", messages.get(0).getMeasurements().get(0));
        assertEquals("humidity", messages.get(1).getMeasurements().get(0));
        assertEquals("status", messages.get(2).getMeasurements().get(0));
    }

    @Test
    public void testAllDataTypes() throws Exception {
        Date timestamp = new Date();
        List<Metric> metrics = new ArrayList<>();
        SparkplugBPayload payload = new SparkplugBPayload(
            timestamp, metrics, 0L, UUID.randomUUID().toString(), null
        );
        
        // Test all supported SparkplugB data types
        payload.addMetric(new Metric.MetricBuilder("Int8", MetricDataType.Int8, (byte)1).createMetric());
        payload.addMetric(new Metric.MetricBuilder("Int16", MetricDataType.Int16, (short)2).createMetric());
        payload.addMetric(new Metric.MetricBuilder("Int32", MetricDataType.Int32, 3).createMetric());
        payload.addMetric(new Metric.MetricBuilder("Int64", MetricDataType.Int64, 4L).createMetric());
        payload.addMetric(new Metric.MetricBuilder("UInt8", MetricDataType.UInt8, (short)5).createMetric());
        payload.addMetric(new Metric.MetricBuilder("UInt16", MetricDataType.UInt16, 6).createMetric());
        payload.addMetric(new Metric.MetricBuilder("UInt32", MetricDataType.UInt32, 7L).createMetric());
        payload.addMetric(new Metric.MetricBuilder("UInt64", MetricDataType.UInt64, BigInteger.valueOf(8)).createMetric());
        payload.addMetric(new Metric.MetricBuilder("Float", MetricDataType.Float, 9.0f).createMetric());
        payload.addMetric(new Metric.MetricBuilder("Double", MetricDataType.Double, 10.0).createMetric());
        payload.addMetric(new Metric.MetricBuilder("Boolean", MetricDataType.Boolean, true).createMetric());
        payload.addMetric(new Metric.MetricBuilder("String", MetricDataType.String, "test").createMetric());
        
        byte[] bytes = new SparkplugBPayloadEncoder().getBytes(payload, false);
        ByteBuf byteBuf = Unpooled.wrappedBuffer(bytes);

        List<Message> messages = formatter.format(byteBuf);
        assertNotNull(messages);
        assertEquals(12, messages.size()); // One message per metric
        
        // Verify each message
        assertEquals("1", messages.get(0).getValues().get(0));
        assertEquals("int8", messages.get(0).getMeasurements().get(0));
        
        assertEquals("2", messages.get(1).getValues().get(0));
        assertEquals("int16", messages.get(1).getMeasurements().get(0));
        
        assertEquals("3", messages.get(2).getValues().get(0));
        assertEquals("int32", messages.get(2).getMeasurements().get(0));
        
        assertEquals("4", messages.get(3).getValues().get(0));
        assertEquals("int64", messages.get(3).getMeasurements().get(0));
        
        assertEquals("0", messages.get(4).getValues().get(0));
        assertEquals("uint8", messages.get(4).getMeasurements().get(0));
        
        assertEquals("0", messages.get(5).getValues().get(0));
        assertEquals("uint16", messages.get(5).getMeasurements().get(0));
        
        assertEquals("7", messages.get(6).getValues().get(0));
        assertEquals("uint32", messages.get(6).getMeasurements().get(0));
        
        assertEquals("8", messages.get(7).getValues().get(0));
        assertEquals("uint64", messages.get(7).getMeasurements().get(0));
        
        assertEquals("9.000000", messages.get(8).getValues().get(0));
        assertEquals("float", messages.get(8).getMeasurements().get(0));
        
        assertEquals("10.000000", messages.get(9).getValues().get(0));
        assertEquals("double", messages.get(9).getMeasurements().get(0));
        
        assertEquals("true", messages.get(10).getValues().get(0));
        assertEquals("boolean", messages.get(10).getMeasurements().get(0));
        
        assertEquals("test", messages.get(11).getValues().get(0));
        assertEquals("string", messages.get(11).getMeasurements().get(0));
    }

    @Test
    public void testCustomPropertyNames() throws Exception {
        Date timestamp = new Date();
        List<Metric> metrics = new ArrayList<>();
        SparkplugBPayload payload = new SparkplugBPayload(
            timestamp, metrics, 0L, UUID.randomUUID().toString(), null
        );
        
        // Create a metric with custom property names
        PropertySet properties = new PropertySet();
        properties.put("GroupID", new PropertyValue<>(PropertyDataType.String, "TestGroup"));
        properties.put("EdgeNodeID", new PropertyValue<>(PropertyDataType.String, "TestEdge"));
        properties.put("AgentID", new PropertyValue<>(PropertyDataType.String, "TestDevice"));
        
        Metric metric = new Metric.MetricBuilder("Temperature", MetricDataType.Float, 25.5f)
            .properties(properties)
            .createMetric();
        payload.addMetric(metric);
        
        byte[] bytes = new SparkplugBPayloadEncoder().getBytes(payload, false);
        ByteBuf byteBuf = Unpooled.wrappedBuffer(bytes);

        List<Message> messages = formatter.format(byteBuf);
        assertNotNull(messages);
        assertEquals(1, messages.size());
        
        // Verify the device path contains the custom property values
        String devicePath = messages.get(0).getDevice();
        assertTrue(devicePath.contains("test_group"));
        assertTrue(devicePath.contains("test_edge"));
        assertTrue(devicePath.contains("test_device"));
        assertEquals("25.500000", messages.get(0).getValues().get(0));
        assertEquals("temperature", messages.get(0).getMeasurements().get(0));
    }
    
    @Test
    public void testMixedPropertyNames() throws Exception {
        Date timestamp = new Date();
        List<Metric> metrics = new ArrayList<>();
        SparkplugBPayload payload = new SparkplugBPayload(
            timestamp, metrics, 0L, UUID.randomUUID().toString(), null
        );
        
        // Create a metric with mixed property names (some original, some custom)
        PropertySet properties = new PropertySet();
        properties.put("GroupID", new PropertyValue<>(PropertyDataType.String, "TestGroup"));
        properties.put("edge", new PropertyValue<>(PropertyDataType.String, "TestEdge"));
        properties.put("AgentID", new PropertyValue<>(PropertyDataType.String, "TestDevice"));
        
        Metric metric = new Metric.MetricBuilder("Temperature", MetricDataType.Float, 25.5f)
            .properties(properties)
            .createMetric();
        payload.addMetric(metric);
        
        byte[] bytes = new SparkplugBPayloadEncoder().getBytes(payload, false);
        ByteBuf byteBuf = Unpooled.wrappedBuffer(bytes);

        List<Message> messages = formatter.format(byteBuf);
        assertNotNull(messages);
        assertEquals(1, messages.size());
        
        // Verify the device path contains all property values
        String devicePath = messages.get(0).getDevice();
        assertTrue(devicePath.contains("test_group"));
        assertTrue(devicePath.contains("test_edge"));
        assertTrue(devicePath.contains("test_device"));
        assertEquals("25.500000", messages.get(0).getValues().get(0));
        assertEquals("temperature", messages.get(0).getMeasurements().get(0));
    }
} 