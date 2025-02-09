package org.factoryiq.iotdb.sparkplugb.formatter;

import org.apache.iotdb.db.protocol.mqtt.PayloadFormatter;
import org.apache.iotdb.db.protocol.mqtt.Message;
import io.netty.buffer.ByteBuf;
import org.eclipse.tahu.protobuf.SparkplugBProto;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class CustomizedSparkplugPayloadFormatter implements PayloadFormatter {

    private static final Logger LOGGER = LoggerFactory.getLogger(CustomizedSparkplugPayloadFormatter.class);
    // Default device if we cannot extract properties
    private static final String DEFAULT_DEVICE = "root.mqtt.sparkplugb";

    @Override
    public List<Message> format(ByteBuf payload) {
        if (payload == null || !payload.isReadable()) {
            LOGGER.warn("Received null or empty payload");
            return Collections.emptyList();
        }

        try {
            byte[] bytes = new byte[payload.readableBytes()];
            payload.readBytes(bytes);
            
            // Decoding Sparkplug B payload using protobuf
            SparkplugBProto.Payload protoPayload = SparkplugBProto.Payload.parseFrom(bytes);
            List<Message> messages = convertProtoMetricsToMessages(protoPayload);
            
            // Log successful conversion
            if (!messages.isEmpty()) {
                LOGGER.info("Successfully converted {} metrics:", messages.size());
                for (Message msg : messages) {
                    LOGGER.info("Device: {}, Measurement: {}, Value: {}, Timestamp: {}", 
                        msg.getDevice(),
                        msg.getMeasurements().get(0),
                        msg.getValues().get(0),
                        msg.getTimestamp());
                }
            } else {
                LOGGER.warn("No metrics were converted from the payload");
            }
            
            return messages;
        } catch (Exception e) {
            LOGGER.error("Error parsing Sparkplug B payload: {}", e.getMessage());
            return Collections.emptyList();
        }
    }

    private List<Message> convertProtoMetricsToMessages(SparkplugBProto.Payload payload) {
        List<Message> messages = new ArrayList<>();
        long timestamp = payload.getTimestamp();
        
        LOGGER.debug("Processing {} metrics from payload", payload.getMetricsCount());
        
        for (SparkplugBProto.Payload.Metric metric : payload.getMetricsList()) {
            Message message = createMessageFromMetric(metric, timestamp);
            if (message != null) {
                messages.add(message);
            }
        }
        return messages;
    }

    /**
     * Build a unified device namespace from the Sparkplug metric properties if available.
     * The expected keys are "group", "edge" and "device". The resulting device path will be:
     * root.mqtt.sparkplugb.<group>.<edge>.<device>
     */
    private String extractDeviceFromProperties(SparkplugBProto.Payload.Metric metric) {
        if (metric.hasProperties()) {
            SparkplugBProto.Payload.PropertySet properties = metric.getProperties();
            List<String> keys = properties.getKeysList();
            List<SparkplugBProto.Payload.PropertyValue> values = properties.getValuesList();

            String group = null;
            String edge = null;
            String device = null;

            for (int i = 0; i < keys.size(); i++) {
                if (i >= values.size()) break;
                
                String key = keys.get(i);
                SparkplugBProto.Payload.PropertyValue value = values.get(i);
                
                if (value.getType() == 12) { // String type
                    switch (key) {
                        case "group":
                            group = normalizeString(value.getStringValue());
                            break;
                        case "edge":
                            edge = normalizeString(value.getStringValue());
                            break;
                        case "device":
                            device = normalizeString(value.getStringValue());
                            break;
                    }
                }
            }

            if (group != null && edge != null && device != null) {
                String devicePath = String.format("root.mqtt.sparkplugb.%s.%s.%s", group, edge, device);
                LOGGER.debug("Created device path: {}", devicePath);
                return devicePath;
            }
        }
        LOGGER.warn("Could not extract device info from properties for metric: {}, using default", metric.getName());
        return DEFAULT_DEVICE;
    }

    private Message createMessageFromMetric(SparkplugBProto.Payload.Metric metric, long timestamp) {
        try {
            Message message = new Message();
            String device = extractDeviceFromProperties(metric);
            message.setDevice(device);
            String normalizedName = normalizeString(metric.getName());
            message.setMeasurements(Collections.singletonList(normalizedName));
            
            // Use metric timestamp if available, otherwise use payload timestamp
            long metricTime = metric.hasTimestamp() ? metric.getTimestamp() : timestamp;
            if (metricTime <= 0) {
                metricTime = System.currentTimeMillis();
            }
            message.setTimestamp(metricTime);

            int dataType = metric.getDatatype();
            String value = null;
            
            switch (dataType) {
                case 1: // Int8
                case 2: // Int16
                case 3: // Int32
                    value = String.valueOf(metric.getIntValue());
                    break;
                case 4: // Int64
                case 5: // UInt8
                case 6: // UInt16
                case 7: // UInt32
                case 8: // UInt64
                    value = String.valueOf(metric.getLongValue());
                    break;
                case 9: // Float
                    value = String.format("%.6f", metric.getFloatValue());
                    break;
                case 10: // Double
                    value = String.format("%.6f", metric.getDoubleValue());
                    break;
                case 11: // Boolean
                    value = String.valueOf(metric.getBooleanValue());
                    break;
                case 12: // String
                case 13: // Text
                    value = normalizeString(metric.getStringValue());
                    break;
                default:
                    value = String.valueOf(metric.getDoubleValue());
                    break;
            }
            
            message.setValues(Collections.singletonList(value));
            
            LOGGER.debug("Created message - Device: {}, Measurement: {}, Value: {}, Time: {}", 
                device, normalizedName, value, metricTime);
            
            return message;
        } catch (Exception e) {
            LOGGER.error("Error creating message for metric {}: {}", metric.getName(), e.getMessage());
            return null;
        }
    }

    /**
     * Normalizes strings by replacing spaces with underscores, converting to lowercase,
     * and removing any invalid characters
     * @param input The input string to normalize
     * @return The normalized string
     */
    private String normalizeString(String input) {
        if (input == null) {
            return "";
        }
        // Replace spaces with underscores, convert to lowercase, and remove any other potentially problematic characters
        return input.trim()
                   .toLowerCase()
                   .replaceAll("\\s+", "_")
                   .replaceAll("[^a-z0-9_-]", "_");
    }

    @Override
    public String getName() {
        return "CustomizedSparkplugB";
    }
}
