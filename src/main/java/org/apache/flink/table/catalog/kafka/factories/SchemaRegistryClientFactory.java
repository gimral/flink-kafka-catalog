package org.apache.flink.table.catalog.kafka.factories;

import io.confluent.kafka.schemaregistry.SchemaProvider;
import io.confluent.kafka.schemaregistry.avro.AvroSchemaProvider;
import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.json.JsonSchemaProvider;
import io.confluent.kafka.schemaregistry.testutil.MockSchemaRegistry;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

public class SchemaRegistryClientFactory {
    public SchemaRegistryClientFactory(){

    }
    public SchemaRegistryClient get(List<String> urls,int maxSchemaObject, Map<String,String> properties) {
        List<SchemaProvider> providers = Arrays.asList(new AvroSchemaProvider(),new JsonSchemaProvider());
        return get(urls,maxSchemaObject,properties,providers);
    }

    public SchemaRegistryClient get(List<String> urls,int maxSchemaObject, Map<String,String> properties,List<SchemaProvider> providers) {
        String mockScope = MockSchemaRegistry.validateAndMaybeGetMockScope(urls);
        if (mockScope != null) {
            return MockSchemaRegistry.getClientForScope(mockScope,providers);
        } else {
            return new CachedSchemaRegistryClient(
                    urls,
                    maxSchemaObject,
                    providers,
                    properties
            );
        }
    }
}
