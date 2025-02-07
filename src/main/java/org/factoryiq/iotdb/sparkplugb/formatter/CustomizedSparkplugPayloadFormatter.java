package org.factoryiq.iotdb.sparkplugb.formatter;

import org.apache.iotdb.db.protocol.mqtt.PayloadFormatter;
import org.apache.iotdb.db.protocol.mqtt.Message;
import org.eclipse.tahu.message.SparkplugBPayloadDecoder;
import org.eclipse.tahu.message.model.SparkplugBPayload;
import org.eclipse.tahu.message.model.Metric;
import io.netty.buffer.ByteBuf;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;



public class CustomizedSparkplugPayloadFormatter implements PayloadFormatter {
    private static final Logger LOGGER = LoggerFactory.getLogger(CustomizedSparkplugPayloadFormatter.class);

    @Override
    public List<Message> format(ByteBuf payload) {
        if (payload == null) {
            return Collections.emptyList();
        }

        try {
            // Convert ByteBuf to byte array for Tahu parsing
            byte[] bytes = new byte[payload.readableBytes()];
            payload.readBytes(bytes);

            // Parse the Sparkplug B payload using Tahu decoder
            SparkplugBPayloadDecoder decoder = new SparkplugBPayloadDecoder();
            SparkplugBPayload sparkplugPayload = decoder.buildFromByteArray(bytes, null);
            
            List<Message> messages = new ArrayList<>();
            
            for (Metric metric : sparkplugPayload.getMetrics()) {
                Message message = new Message();
                message.setMeasurements(Collections.singletonList(metric.getName()));
                
                String value = extractValue(metric);
                if (value != null) {
                    message.setValues(Collections.singletonList(value));
                } else {
                    message.setValues(Collections.singletonList("null"));
                }
                
                message.setTimestamp(System.currentTimeMillis());
                messages.add(message);
            }
            
            return messages;
        } catch (Exception e) {
            LOGGER.error("Error parsing Sparkplug B payload", e);
            return Collections.emptyList();
        }
    }

    private String extractValue(Metric metric) {
        if (metric.getValue() == null) {
            return null;
        }

        return metric.getValue().toString();
    }

    @Override
    public String getName() {
        return "CustomizedSparkplugB";
    }
}
