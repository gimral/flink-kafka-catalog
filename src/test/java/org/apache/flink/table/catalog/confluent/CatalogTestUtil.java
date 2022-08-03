package org.apache.flink.table.catalog.confluent;

import io.confluent.kafka.schemaregistry.avro.AvroSchema;
import io.confluent.kafka.schemaregistry.json.JsonSchema;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.flink.table.catalog.ObjectPath;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class CatalogTestUtil {
    public final static List<String> SCHEMA_REGISTRY_URIS = Collections.singletonList("mock://");
    public final static String CATALOG_NAME = "kafka";

    public final static String table1 = "t1";
    public final static ObjectPath table1Path = new ObjectPath(ConfluentSchemaRegistryCatalog.DEFAULT_DB, table1);
    private final static Schema table1Schema = SchemaBuilder
            .record(table1)
            .fields()
            .name("name")
            .type(Schema.create(Schema.Type.STRING))
            .noDefault()
            .name("age")
            .type(Schema.create(Schema.Type.INT))
            .noDefault()
            .endRecord();
    public final static AvroSchema table1AvroSchema = new AvroSchema(table1Schema);
    public final static GenericRecord table1Message = new GenericRecordBuilder(table1Schema)
            .set("name","Abcd")
            .set("age",30)
            .build();


    public final static String table2 = "t2";
    public final static String table2_target = "table2_target";
    public final static ObjectPath table2Path = new ObjectPath(ConfluentSchemaRegistryCatalog.DEFAULT_DB, table2);
    public final static String table2Identifier = "`" + CATALOG_NAME + "`.`" + ConfluentSchemaRegistryCatalog.DEFAULT_DB + "`." + table2;
    public final static String table2_targetIdentifier = "`" + CATALOG_NAME + "`.`" + ConfluentSchemaRegistryCatalog.DEFAULT_DB + "`." + table2_target;
    public final static JsonSchema table2JsonSchema = new JsonSchema("{\n" +
            "  \"title\": \""+table2+"\",\n" +
            "  \"type\": \"object\",\n" +
            "  \"properties\": {\n" +
            "    \"name\": {\n" +
            "      \"type\": \"string\",\n" +
            "      \"description\": \"The name.\"\n" +
            "    },\n" +
            "    \"age\": {\n" +
            "      \"description\": \"Age in years.\",\n" +
            "      \"type\": \"string\"\n" +
            "    }\n" +
            "  }\n" +
            "}"
    );
    public final static String table2Message = "{\"name\":\"Abcd\",\"age\":\"30\"}";
}
