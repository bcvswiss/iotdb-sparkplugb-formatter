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
- **Unified Namespace:** Creates a unified IoTDB namespace based on metric properties

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

## Namespace Convention

The formatter creates a unified namespace in IoTDB by extracting information from the metric properties. Device paths follow this structure:

```
root.mqtt.sparkplugb.<group_id>.<edge_node_id>.<device_id>
```

**Example:**

```
root.mqtt.sparkplugb.factoryIQ.edge123.modbus123
```

## Required Metric Properties

Each Sparkplug B metric must include these properties for proper namespace creation:

### Original Property Names
```
"properties": {
    "keys": ["group", "edge", "device"],
    "values": [
        {"type": 12, "stringValue": "factoryIQ"},
        {"type": 12, "stringValue": "edge123"},
        {"type": 12, "stringValue": "modbus123"}
    ]
}
```

### Custom Property Names
This formatter also supports an alternative set of property names:
```
"properties": {
    "keys": ["GroupID", "EdgeNodeID", "AgentID"],
    "values": [
        {"type": 12, "stringValue": "factoryIQ"},
        {"type": 12, "stringValue": "edge123"},
        {"type": 12, "stringValue": "modbus123"}
    ]
}
```

The formatter will first check for the custom property names, and if not found, will fall back to the original property names. This ensures backward compatibility while supporting systems that use the custom naming convention.

If these properties are missing, the formatter defaults to: `root.mqtt.sparkplugb`

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

### Example Payload

A Sparkplug B payload with multiple metrics:

```json
{
  "timestamp": 1234567890,
  "metrics": [
    {
      "name": "Temperature",
      "type": "Float",
      "value": 23.5,
      "properties": {
          "keys": ["group", "edge", "device"],
          "values": [
              {"type": 12, "stringValue": "factoryIQ"},
              {"type": 12, "stringValue": "edge123"},
              {"type": 12, "stringValue": "modbus123"}
          ]
      }
    },
    {
      "name": "Status",
      "type": "Boolean",
      "value": true,
      "properties": {
          "keys": ["group", "edge", "device"],
          "values": [
              {"type": 12, "stringValue": "factoryIQ"},
              {"type": 12, "stringValue": "edge123"},
              {"type": 12, "stringValue": "modbus123"}
          ]
      }
    },
    {
      "name": "DeviceID",
      "type": "String",
      "value": "Device1",
      "properties": {
          "keys": ["group", "edge", "device"],
          "values": [
              {"type": 12, "stringValue": "factoryIQ"},
              {"type": 12, "stringValue": "edge123"},
              {"type": 12, "stringValue": "modbus123"}
          ]
      }
    }
  ]
}
```

This payload is converted to IoTDB measurements such as:

```sql
Temperature: 23.5
Status: true
DeviceID: "Device1"
```

## Logging

The formatter provides detailed logging at different levels:

- **INFO**: Successfully converted metrics with device path, measurement, value, and timestamp.
- **DEBUG**: Detailed conversion process and property extraction.
- **WARN**: Missing properties or conversion issues.

## Testing

Run the test suite to verify formatter functionality:

```bash
mvn test
```

## License

This project is licensed under the Apache License 2.0 - see the [LICENSE](LICENSE) file for details.