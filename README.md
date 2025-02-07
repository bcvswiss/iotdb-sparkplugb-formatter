# IoTDB Sparkplug B Formatter

A custom payload formatter for Apache IoTDB that handles Sparkplug B binary messages.

## Features

- Decodes Sparkplug B protobuf messages into IoTDB timeseries data
- Supports all Sparkplug B data types including:
    - Numeric types (Int8/16/32/64, UInt8/16/32/64, Float, Double)
    - Boolean
    - String
    - Complex types with properties
- Handles null values
- Compatible with both Modbus and OPC UA payloads
- Preserves metric names and timestamps

## Installation

1. Build the formatter:

```bash
mvn clean package
```
2. Create the MQTT extension directory if it doesn't exist:
```bash
mkdir -p ${IOTDB_HOME}/ext/mqtt/
```

3. Copy the generated JAR (from `target/iotdb-sparkplugb-formatter-1.0.0.jar`) to IoTDB's MQTT extension directory:

```bash
cp target/iotdb-sparkplugb-formatter-1.0.0.jar ${IOTDB_HOME}/ext/mqtt/
```

4. Configure IoTDB to use the formatter by adding these lines to `iotdb-datanode.properties`:

```properties
enable_mqtt_service=true
mqtt_payload_formatter=CustomizedSparkplugB
```

5. Restart your IoTDB server to load the new formatter.

### Topic Format Explanation

Sparkplug B topics follow this format:

- `spBv1.0`: Sparkplug B protocol version
- `<group_id>`: Logical grouping of MQTT Edge of Network (EoN) nodes
- `<message_type>`: Type of message (NBIRTH, NDEATH, DBIRTH, DDEATH, NDATA, DDATA)
- `<edge_node_id>`: Identity of the edge node
- `<device_id>`: Identity of the device (optional)

Common message types:

- `NBIRTH`: Edge node birth certificate
- `NDEATH`: Edge node death certificate
- `DBIRTH`: Device birth certificate
- `DDEATH`: Device death certificate
- `NDATA`: Edge node data
- `DDATA`: Device data

## Usage

The formatter automatically processes incoming Sparkplug B messages and converts them to IoTDB timeseries data. Each
metric in the Sparkplug B payload becomes a separate measurement in IoTDB.

### Metric Mapping

- Each Sparkplug B metric is converted to an IoTDB measurement
- Metric names are preserved as-is
- Metric values are converted to appropriate string representations
- Null values are handled gracefully
- Timestamps are preserved from the original messages

### Supported Data Types

The formatter handles all Sparkplug B data types:

- Int8, Int16, Int32, Int64
- UInt8, UInt16, UInt32, UInt64
- Float, Double
- Boolean
- String
- Complex types with properties (e.g., Modbus registers, OPC UA values)

### Example

A Sparkplug B payload with multiple metrics:

```json
{
  "timestamp": 1234567890,
  "metrics": [
    {
      "name": "Temperature",
      "type": "Float",
      "value": 23.5
    },
    {
      "name": "Status",
      "type": "Boolean",
      "value": true
    },
    {
      "name": "DeviceID",
      "type": "String",
      "value": "Device1"
    }
  ]
}
```

Will be converted to IoTDB measurements:

```sql
Temperature: 23.5
Status: true
DeviceID: "Device1"
```

## Testing

Run the test suite to verify formatter functionality:

```bash
mvn test
```

## License

This project is licensed under the Apache License 2.0 - see the [LICENSE](LICENSE) file for details.

