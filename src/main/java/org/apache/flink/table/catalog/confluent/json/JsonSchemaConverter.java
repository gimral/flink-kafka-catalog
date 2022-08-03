package org.apache.flink.table.catalog.confluent.json;

import com.fasterxml.jackson.databind.JsonNode;
import io.confluent.kafka.schemaregistry.json.JsonSchema;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.types.DataType;
import org.apache.flink.util.Preconditions;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public class JsonSchemaConverter {

    private JsonSchemaConverter() {
        // private
    }

    public static DataType convertToDataType(JsonSchema jsonSchema) {
        Preconditions.checkNotNull(jsonSchema, "Json schema must not be null.");
        return convertToDataType(jsonSchema.toJsonNode());
    }

    private static DataType convertToDataType(JsonNode node) {
        String type = node.get("type").asText("");
        switch(type) {
            case "object":
                if(!node.has("properties"))
                    throw new IllegalArgumentException("Json object does not have properties field.");
                JsonNode properties = node.get("properties");
                List<Map.Entry<String, JsonNode>> schemaFields = new ArrayList<>();
                properties.fields().forEachRemaining(schemaFields::add);
                DataTypes.Field[] fields = new DataTypes.Field[schemaFields.size()];
                for (int i = 0; i < schemaFields.size(); ++i) {
                    Map.Entry<String, JsonNode> field = schemaFields.get(i);
                    fields[i] = DataTypes.FIELD(field.getKey(), convertToDataType(field.getValue()));
                }
                return DataTypes.ROW(fields).notNull();

            case "array":
                if(!node.has("items"))
                    throw new IllegalArgumentException("Json array does not have items field.");
                return DataTypes.ARRAY(convertToDataType(node.get("items"))).notNull();
//            case MAP:
//                return (DataType)DataTypes.MAP((DataType)DataTypes.STRING().notNull(), convertToDataType(schema.getValueType())).notNull();
//            case UNION:
//                Schema actualSchema;
//                boolean nullable;
//                if (schema.getTypes().size() == 2 && ((Schema)schema.getTypes().get(0)).getType() == Schema.Type.NULL) {
//                    actualSchema = (Schema)schema.getTypes().get(1);
//                    nullable = true;
//                } else if (schema.getTypes().size() == 2 && ((Schema)schema.getTypes().get(1)).getType() == Schema.Type.NULL) {
//                    actualSchema = (Schema)schema.getTypes().get(0);
//                    nullable = true;
//                } else {
//                    if (schema.getTypes().size() != 1) {
//                        return new AtomicDataType(new TypeInformationRawType(false, Types.GENERIC(Object.class)));
//                    }
//
//                    actualSchema = (Schema)schema.getTypes().get(0);
//                    nullable = false;
//                }
//
//                DataType converted = convertToDataType(actualSchema);
//                return nullable ? (DataType)converted.nullable() : converted;
            case "string":
                if(node.has("format")){
                    String format = node.get("format").asText();
                    if(Objects.equals(format, "date"))
                        return DataTypes.DATE().notNull();
                    else if(Objects.equals(format, "date-time"))
                        return DataTypes.TIMESTAMP().notNull();
                    else if(Objects.equals(format, "datetime-local"))
                        return DataTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE().notNull();
                    else if(Objects.equals(format, "time"))
                        return DataTypes.TIME().notNull();

                }
                return DataTypes.STRING().notNull();
            case "integer":
                return DataTypes.INT().notNull();
            case "number":
                return DataTypes.DECIMAL(38, 8).notNull();
            case "boolean":
                return DataTypes.BOOLEAN().notNull();
            case "null":
                return DataTypes.NULL();
            default:
                throw new IllegalArgumentException("Unsupported Json type '" + type + "'.");
        }
    }

}
