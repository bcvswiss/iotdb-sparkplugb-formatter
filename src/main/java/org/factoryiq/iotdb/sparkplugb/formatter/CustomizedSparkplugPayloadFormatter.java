package org.factoryiq.iotdb.sparkplugb.formatter;

import org.apache.iotdb.db.protocol.mqtt.PayloadFormatter;
import org.apache.iotdb.db.protocol.mqtt.Message;
import io.netty.buffer.ByteBuf;
import org.eclipse.tahu.protobuf.SparkplugBProto;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.*;
import com.google.common.collect.Lists;

public class CustomizedSparkplugPayloadFormatter implements PayloadFormatter {
    private static final Logger LOGGER = LoggerFactory.getLogger(CustomizedSparkplugPayloadFormatter.class);
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
            
            SparkplugBProto.Payload protoPayload = SparkplugBProto.Payload.parseFrom(bytes);
            
            if (protoPayload.getMetricsCount() == 0) {
                LOGGER.warn("Payload contains no metrics");
                return Collections.emptyList();
            }

            List<Message> messages = new ArrayList<>();
            for (SparkplugBProto.Payload.Metric metric : protoPayload.getMetricsList()) {
                try {
                    Message message = createMessageFromMetric(metric);
                    if (message != null && isValidMessage(message)) {
                        messages.add(message);
                    }
                } catch (Exception e) {
                    LOGGER.error("Error processing metric {}: {}", metric.getName(), e.getMessage(), e);
                }
            }
            
            return messages;
        } catch (Exception e) {
            LOGGER.error("Error parsing Sparkplug B payload: {}", e.getMessage(), e);
            return Collections.emptyList();
        }
    }

    private Message createMessageFromMetric(SparkplugBProto.Payload.Metric metric) {
        try {
            Message message = new Message();
            String device = extractDeviceFromProperties(metric);
            message.setDevice(device);
            
            String normalizedName = normalizeString(metric.getName());
            message.setMeasurements(Lists.newArrayList(normalizedName));

            long metricTimestamp = convertSparkplugTimestamp(metric.getTimestamp());
            message.setTimestamp(metricTimestamp > 0 ? metricTimestamp : System.currentTimeMillis());

            String value = convertMetricValue(metric);
            message.setValues(Lists.newArrayList(value));
                
            return message;
        } catch (Exception e) {
            LOGGER.error("Error creating message for metric {}: {}", metric.getName(), e.getMessage(), e);
            return null;
        }
    }

    private String convertMetricValue(SparkplugBProto.Payload.Metric metric) {
        try {
            switch (metric.getDatatype()) {
                case 0:  // Unknown/Number - treat as Double
                case 10: // Double
                    return String.format("%.6f", metric.getDoubleValue());
                case 1: // Int8
                case 2: // Int16
                case 3: // Int32
                    return String.valueOf(metric.getIntValue());
                case 4: // Int64/UInt32
                case 5: // UInt8
                case 6: // UInt16
                case 7: // UInt32
                case 8: // UInt64
                    return String.valueOf(metric.getLongValue());
                case 9: // Float
                    return String.format("%.6f", metric.getFloatValue());
                case 11: // Boolean
                    return String.valueOf(metric.getBooleanValue());
                case 12: // String
                case 13: // Text
                    return normalizeValue(metric.getStringValue());
                default:
                    LOGGER.warn("Unexpected datatype {} for metric {}, defaulting to double", 
                        metric.getDatatype(), metric.getName());
                    return String.format("%.6f", metric.getDoubleValue());
            }
        } catch (Exception e) {
            LOGGER.error("Error converting value for metric {}: {}", metric.getName(), e.getMessage());
            return "null";
        }
    }

    private long convertSparkplugTimestamp(long timestamp) {
        try {
            return timestamp > 0 ? timestamp : System.currentTimeMillis();
        } catch (Exception e) {
            LOGGER.error("Error converting timestamp: {}", e.getMessage());
            return System.currentTimeMillis();
        }
    }

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
                        case "GroupID":  // Custom property name
                            group = normalizeString(value.getStringValue());
                            break;
                        case "edge":
                            edge = normalizeString(value.getStringValue());
                            break;
                        case "EdgeNodeID":  // Custom property name
                            edge = normalizeString(value.getStringValue());
                            break;
                        case "device":
                            device = normalizeString(value.getStringValue());
                            break;
                        case "AgentID":  // Custom property name
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

    private boolean isValidMessage(Message message) {
        if (message.getDevice() == null || !message.getDevice().startsWith("root.")) {
            LOGGER.warn("Invalid device path: {}", message.getDevice());
            return false;
        }
        
        if (message.getMeasurements() == null || message.getMeasurements().isEmpty() ||
            message.getValues() == null || message.getValues().isEmpty() ||
            message.getMeasurements().size() != message.getValues().size()) {
            LOGGER.warn("Invalid measurements or values for device: {}", message.getDevice());
            return false;
        }
        
        if (message.getTimestamp() <= 0) {
            LOGGER.warn("Invalid timestamp for device: {}", message.getDevice());
            return false;
        }
        
        return true;
    }

    private String normalizeString(String input) {
        if (input == null || input.trim().isEmpty()) {
            return "null";
        }
        
        if (input.equals("NullMetric")) {
            return "null";
        }
        
        return input.trim()
            .replaceAll("([a-z])([A-Z])", "$1_$2")
            .replaceAll("\\s+", "_")
            .toLowerCase();
    }

    private String normalizeValue(String value) {
        if (value == null || value.trim().isEmpty()) {
            return "null";
        }
        return value.replaceAll("\\s+", "_");
    }

    @Override
    public String getName() {
        return "CustomizedSparkplugB";
    }
}
