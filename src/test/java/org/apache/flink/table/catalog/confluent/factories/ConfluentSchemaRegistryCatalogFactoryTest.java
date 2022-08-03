package org.apache.flink.table.catalog.confluent.factories;

import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.catalog.Catalog;
import org.apache.flink.table.catalog.CommonCatalogOptions;
import org.apache.flink.table.catalog.confluent.ConfluentSchemaRegistryCatalog;
import org.apache.flink.table.factories.FactoryUtil;
//import org.apache.flink.mock.Whitebox;
import org.junit.Test;

import java.util.*;

import static org.junit.Assert.*;

public class ConfluentSchemaRegistryCatalogFactoryTest {
    private final static List<String> SCHEMA_REGISTRY_URIS = Collections.singletonList("mock://");
    private final static String CATALOG_NAME = "TEST_CATALOG";
    @Test
    public void testCreateCatalogFromFactory() {
        final Map<String, String> options = new HashMap<>();
        options.put(CommonCatalogOptions.CATALOG_TYPE.key(), ConfluentSchemaRegistryCatalogFactoryOptions.IDENTIFIER);
        options.put(ConfluentSchemaRegistryCatalogFactoryOptions.BOOTSTRAP_SERVERS.key(), "kafka");
        options.put(ConfluentSchemaRegistryCatalogFactoryOptions.SCHEMA_REGISTRY_URI.key(),String.join(", ", SCHEMA_REGISTRY_URIS));
        options.put("kafka.properties.group.id", "test");

        final Catalog actualCatalog = FactoryUtil.createCatalog(CATALOG_NAME, options, null, Thread.currentThread().getContextClassLoader());

        assertTrue(actualCatalog instanceof ConfluentSchemaRegistryCatalog);
        assertEquals(((ConfluentSchemaRegistryCatalog) actualCatalog).getName(), CATALOG_NAME);
        assertEquals(((ConfluentSchemaRegistryCatalog) actualCatalog).getDefaultDatabase(), ConfluentSchemaRegistryCatalog.DEFAULT_DB);
//        assertEquals(Whitebox.getInternalState(actualCatalog, "properties"),
//                Whitebox.getInternalState(CATALOG, "properties"));
    }

    @Test
    public void testCreateCatalogFromFactoryFailsIfRegistryURIIsMissing() {
        final Map<String, String> options = new HashMap<>();
        options.put(CommonCatalogOptions.CATALOG_TYPE.key(), ConfluentSchemaRegistryCatalogFactoryOptions.IDENTIFIER);
        options.put(ConfluentSchemaRegistryCatalogFactoryOptions.BOOTSTRAP_SERVERS.key(), "kafka");

        ValidationException exception = assertThrows(ValidationException.class,() ->
                FactoryUtil.createCatalog(CATALOG_NAME, options, null, Thread.currentThread().getContextClassLoader()));

        assertTrue(exception.getCause().getMessage().contains(ConfluentSchemaRegistryCatalogFactoryOptions.SCHEMA_REGISTRY_URI.key()));
    }
}
