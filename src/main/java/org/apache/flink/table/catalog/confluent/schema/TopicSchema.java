package org.apache.flink.table.catalog.confluent.schema;

import org.apache.flink.table.api.Schema;
import org.apache.flink.util.Preconditions;

public class TopicSchema {
    private final Schema schema;
    private final SchemaType schemaType;

    public TopicSchema(Schema schema, String schemaType) {
        this(schema, Enum.valueOf(SchemaType.class,schemaType));
    }

    public TopicSchema(Schema schema, SchemaType schemaType) {
        Preconditions.checkNotNull(schemaType);
        this.schema = schema;
        this.schemaType = schemaType;
    }

    public Schema getSchema() {
        return schema;
    }

    public SchemaType getSchemaType() {
        return schemaType;
    }

    public enum SchemaType{
        AVRO("avro","avro-confluent"),
        JSON("json","json"),
        RAW("raw","raw");


        private final String value;
        private final String format;

        SchemaType(final String value,final String format) {
            this.value = value;
            this.format = format;
        }

        public String getValue() {
            return value;
        }

        public String getFormat() {
            return format;
        }

        @Override
        public String toString() {
            return this.getValue();
        }
    }
}


