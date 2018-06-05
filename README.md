# Pravega Connectors for Apache NiFi

Connectors to read and write [Pravega](http://pravega.io/) streams with [Apache NiFi](https://nifi.apache.org/).

![PublishPravega-Overview](images/PublishPravega-Overview.png)

![PublishPravega-Configure](images/PublishPravega-Configure.png)


## Processors

  - PublishPravega: This processor writes incoming FlowFiles to a Pravega stream.
    It uses Pravega transactions to provide at-least-one guarantees.
    
  - PublishPravega: This is similar to PublishPravega but it uses a NiFi Record Reader to parse the incoming
    FlowFiles as CSV, JSON, or Avro. Each record will be written as a separate event to a Pravega stream.
    Events written to Pravega will be serialized with a Record Writer and can be CSV, JSON, or Avro.

  - ConsumePravega: This processor reads events from a Pravega stream and produces FlowFiles.


## Building

```
mvn install
```

## Adding to NiFi

Copy the produced artifact `nifi-pravega-nar/target/nifi-pravega-nar-0.1.nar` to the `lib` directory of your NiFi home directory. 
Then restart Apache NiFi.
