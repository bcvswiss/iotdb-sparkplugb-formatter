FROM apache/iotdb:1.3.3-standalone

WORKDIR /iotdb

RUN mkdir -p ext/mqtt

COPY target/iotdb-sparkplugb-formatter-1.0.0.jar ext/mqtt/iotdb-sparkplugb-formatter-1.0.0.jar

# Add MQTT settings to the configuration file if they don't exist
RUN echo "\n\
# MQTT Service Configuration\n\
enable_mqtt_service=true\n\
mqtt_payload_formatter=CustomizedSparkplugB\n\
" >> conf/iotdb-system.properties