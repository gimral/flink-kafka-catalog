package org.apache.flink.table.catalog.kafka;

import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.catalog.CatalogDatabase;
import org.apache.flink.table.catalog.CatalogTable;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.catalog.kafka.factories.KafkaCatalogFactoryOptions;
import org.apache.flink.table.catalog.kafka.factories.KafkaAdminClientFactory;
import org.apache.flink.table.catalog.kafka.factories.SchemaRegistryClientFactory;
import org.apache.flink.table.catalog.exceptions.TableNotExistException;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.ListTopicsResult;
import org.apache.kafka.common.KafkaFuture;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.*;

import static org.apache.flink.table.catalog.kafka.CatalogTestUtil.*;
import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

public class KafkaCatalogTest {

    private KafkaCatalog catalog;

    @Before
    public void init() throws RestClientException, IOException {
        SchemaRegistryClientFactory schemaRegistryClientFactory = new SchemaRegistryClientFactory();
        SchemaRegistryClient schemaRegistryClient = schemaRegistryClientFactory.get(SCHEMA_REGISTRY_URIS,1000,new HashMap<>());
        schemaRegistryClient.register(table1, table1AvroSchema);
        schemaRegistryClient.register(table2, table2JsonSchema);

        Map<String, String> properties = new HashMap<>();
        properties.put("connector", "kafka");
        properties.put(KafkaCatalogFactoryOptions.SCHEMA_REGISTRY_URI.key(), String.join(", ", SCHEMA_REGISTRY_URIS));

        KafkaAdminClientFactory mockAdminClientFactory = mock(KafkaAdminClientFactory.class);
        AdminClient mockAdminClient = mock(AdminClient.class);
        ListTopicsResult mockListTopicsResult = mock(ListTopicsResult.class);
        when(mockListTopicsResult.names()).thenReturn(KafkaFuture.completedFuture(new HashSet<>(Arrays.asList(table1,table2))));
        when(mockAdminClient.listTopics(any())).thenReturn(mockListTopicsResult);
        when(mockAdminClientFactory.get(any())).thenReturn(mockAdminClient);
        catalog = new KafkaCatalog(CATALOG_NAME,properties, mockAdminClientFactory);
        catalog.open();
    }



    @Test
    public void testDefaultDBExists(){
        assertTrue(catalog.databaseExists(KafkaCatalog.DEFAULT_DB));
    }

    @Test
    public void testNonDefaultDBDoesNotExists(){
        assertFalse(catalog.databaseExists("Test"));
    }

    @Test
    public void testGetDbExist() throws Exception {
        CatalogDatabase db = catalog.getDatabase(KafkaCatalog.DEFAULT_DB);
        assertNotNull(db);
    }

    @Test(expected = TableNotExistException.class)
    public void testGetTableNotExist() throws Exception {
        catalog.getTable(new ObjectPath(KafkaCatalog.DEFAULT_DB, "NOT_EXIST"));
    }

    @Test
    public void testListTables() throws Exception {
        List<String> tables = catalog.listTables(KafkaCatalog.DEFAULT_DB);

        assertEquals(2, tables.size());
        assertTrue(tables.contains(table1));
        assertTrue(tables.contains(table2));
    }

    @Test
    public void testTableExists() {
        assertTrue(catalog.tableExists(table1Path));
        assertTrue(catalog.tableExists(table2Path));
    }

    @Test
    public void testTableNotExists() {
        assertFalse(catalog.tableExists(new ObjectPath(KafkaCatalog.DEFAULT_DB, "NOT_EXIST")));
    }

    @Test
    public void testAvroGetTable() throws TableNotExistException {
        CatalogTable table = (CatalogTable) catalog.getTable(table1Path);
        Schema schema = table.getUnresolvedSchema();
        Map<String, String> options = table.getOptions();
        assertEquals("avro-confluent",options.get("format"));
        assertEquals(table1,options.get("topic"));
        assertEquals("kafka",options.get("connector"));
        assertEquals(2,schema.getColumns().size());
        Optional<Schema.UnresolvedColumn> column = schema.getColumns().stream().filter(c -> Objects.equals(c.getName(), "name")).findFirst();
        assertTrue(column.isPresent());
    }

    @Test
    public void testJsonGetTable() throws TableNotExistException {
        CatalogTable table = (CatalogTable) catalog.getTable(table2Path);
        Schema schema = table.getUnresolvedSchema();
        Map<String, String> options = table.getOptions();
        assertEquals("json",options.get("format"));
        assertEquals(table2,options.get("topic"));
        assertEquals("kafka",options.get("connector"));
        assertEquals(5,schema.getColumns().size());
        Optional<Schema.UnresolvedColumn> column = schema.getColumns().stream().filter(c -> Objects.equals(c.getName(), "name")).findFirst();
        assertTrue(column.isPresent());
    }
}
