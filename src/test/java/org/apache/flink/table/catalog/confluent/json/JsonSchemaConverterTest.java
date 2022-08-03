package org.apache.flink.table.catalog.confluent.json;

import io.confluent.kafka.schemaregistry.ParsedSchema;
import io.confluent.kafka.schemaregistry.client.SchemaMetadata;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.confluent.kafka.schemaregistry.json.JsonSchema;
import org.apache.flink.table.catalog.confluent.factories.SchemaRegistryClientFactory;
import org.apache.flink.table.types.DataType;
import org.json.JSONObject;
import org.junit.Test;

import java.io.IOException;
import java.util.HashMap;
import java.util.Optional;

import static org.apache.flink.table.catalog.confluent.CatalogTestUtil.SCHEMA_REGISTRY_URIS;
import static org.apache.flink.table.catalog.confluent.CatalogTestUtil.table2;
import static org.junit.Assert.assertTrue;

public class JsonSchemaConverterTest {

    private static final String jsonSchema = "{\n" +
            "  \"$id\": \"https://example.com/geographical-location.schema.json\",\n" +
            "  \"$schema\": \"https://json-schema.org/draft/2020-12/schema\",\n" +
            "  \"title\": \"Longitude and Latitude Values\",\n" +
            "  \"description\": \"A geographical coordinate.\",\n" +
            "  \"required\": [ \"latitude\", \"longitude\" ],\n" +
            "  \"type\": \"object\",\n" +
            "  \"properties\": {\n" +
            "    \"latitude\": {\n" +
            "      \"type\": \"number\",\n" +
            "      \"minimum\": -90,\n" +
            "      \"maximum\": 90\n" +
            "    },\n" +
            "    \"longitude\": {\n" +
            "      \"type\": \"number\",\n" +
            "      \"minimum\": -180,\n" +
            "      \"maximum\": 180\n" +
            "    },\n" +
            "   \"lastName\": {\n" +
            "      \"type\": \"string\",\n" +
            "      \"description\": \"The person's last name.\"\n" +
            "    },\n" +
            "    \"age\": {\n" +
            "      \"description\": \"Age in years which must be equal to or greater than zero.\",\n" +
            "      \"type\": \"integer\",\n" +
            "      \"minimum\": 0\n" +
            "    },\n" +
            "   \"fruits\": {\n" +
            "      \"type\": \"array\",\n" +
            "      \"items\": {\n" +
            "        \"type\": \"string\"\n" +
            "      }\n" +
            "    },\n" +
            "    \"veggieLike\": {\n" +
            "          \"type\": \"boolean\",\n" +
            "          \"description\": \"Do I like this vegetable?\"\n" +
            "        }\n" +
            "  }\n" +
            "}";

//    @Test
//    public void avroTest() throws RestClientException, IOException {
//        SchemaRegistryClientFactory schemaRegistryClientFactory = new SchemaRegistryClientFactory();
//        SchemaRegistryClient schemaRegistryClient = schemaRegistryClientFactory.get(SCHEMA_REGISTRY_URIS,1000,new HashMap<>());
//        SchemaMetadata latestSchemaMetadata = schemaRegistryClient.getLatestSchemaMetadata(table2);
//        Optional<ParsedSchema> schema2 = schemaRegistryClient.parseSchema(
//                latestSchemaMetadata.getSchemaType(), latestSchemaMetadata.getSchema(), latestSchemaMetadata.getReferences());
//        assertTrue(schema2.isPresent());
//        JSONObject rawSchema = new JSONObject(latestSchemaMetadata.getSchema());
//        DataType dataType = convertToDataType(((JsonSchema) schema2.get()).toJsonNode());
//        System.out.println(dataType.toString());
//
//    }
}
