package org.apache.flink.table.catalog.confluent.json;

import org.apache.flink.table.types.DataType;
import org.junit.Test;

import static org.apache.flink.table.catalog.confluent.CatalogTestUtil.table2JsonSchema;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class JsonSchemaConverterTest {
    @Test
    public void testComplexSchema(){
        DataType dataType = JsonSchemaConverter.convertToDataType(table2JsonSchema);
        assertNotNull(dataType);
        assertEquals(5,dataType.getChildren().size());
    }
}
