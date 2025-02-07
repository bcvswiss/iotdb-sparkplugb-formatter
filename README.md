# IoTDB Sparkplug B Formatter

A custom payload formatter for Apache IoTDB that handles Sparkplug B binary messages.

## Features

- Decodes Sparkplug B protobuf messages
- Maintains original Sparkplug B topic structure
- Supports all Sparkplug B data types
- Preserves metric timestamps

## Installation

1. Build the formatter:

```bash
mvn clean package
```

2. Copy the generated JAR (from `target/iotdb-sparkplugb-formatter-1.0.0.jar`) to IoTDB's MQTT extension directory:

```bash
cp target/iotdb-sparkplugb-formatter-1.0.0.jar ${IOTDB_HOME}/ext/mqtt/
```

3. Configure IoTDB to use the formatter by adding these lines to `iotdb-datanode.properties`:

```properties
enable_mqtt_service=true
mqtt_payload_formatter=CustomizedSparkplugB
```

## Usage

The formatter will automatically handle incoming Sparkplug B messages on topics following the pattern:
```iotdb-sparkplugb-formatter/README.md
spBv1.0/<group_id>/<message_type>/<edge_node_id>/<device_id>
```

Device paths in IoTDB will match the Sparkplug B structure:
```
<group_id>.<edge_node_id>.<device_id>.<metric_name>
```

## Building from Source

```bash
git clone https://github.com/yourusername/iotdb-sparkplugb-formatter.git
cd iotdb-sparkplugb-formatter
mvn clean package
```

## License

This project is licensed under the Apache License 2.0 - see the [LICENSE](LICENSE) file for details.

