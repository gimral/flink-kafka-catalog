package org.apache.flink.table.catalog.confluent.factories;

import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.testutil.MockSchemaRegistry;

import java.util.List;
import java.util.Map;

public class SchemaRegistryClientFactory {
    public SchemaRegistryClientFactory(){

    }
    public SchemaRegistryClient get(List<String> urls,int maxSchemaObject, Map<String,String> properties) {
        String mockScope = MockSchemaRegistry.validateAndMaybeGetMockScope(urls);
        if (mockScope != null) {
            return MockSchemaRegistry.getClientForScope(mockScope);
        } else {
            return new CachedSchemaRegistryClient(
                    urls,
                    maxSchemaObject,
                    properties
            );
        }
    }
}
