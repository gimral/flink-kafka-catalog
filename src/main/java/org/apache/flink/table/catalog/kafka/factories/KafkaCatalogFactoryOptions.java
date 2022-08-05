package org.apache.flink.table.catalog.kafka.factories;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;

public class KafkaCatalogFactoryOptions {
    public static final String IDENTIFIER = "kafka";

    public static final String KAFKA_PREFIX = "properties.";
    public static final String SCHEMA_REGISTRY_PREFIX = "schema.registry.";
    public static final String SCAN_PREFIX = "scan.";
    public static final String SINK_PREFIX = "sink.";


    public static final ConfigOption<String> BOOTSTRAP_SERVERS =
            ConfigOptions.key("properties.bootstrap.servers").stringType().noDefaultValue().withDescription("Required Bootstrap Servers");

    public static final ConfigOption<String> SCHEMA_REGISTRY_URI =
            ConfigOptions.key("schema.registry.uri").stringType().noDefaultValue().withDescription("Required Schema Registry URI");
}
