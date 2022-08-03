package org.apache.flink.table.catalog.confluent;

import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.github.embeddedkafka.EmbeddedKafka;
import io.github.embeddedkafka.EmbeddedKafka$;
import io.github.embeddedkafka.EmbeddedKafkaConfig;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.catalog.Catalog;
import org.apache.flink.table.catalog.confluent.factories.ConfluentSchemaRegistryCatalogFactoryOptions;
import org.apache.flink.table.catalog.confluent.factories.SchemaRegistryClientFactory;
import org.apache.flink.test.util.MiniClusterWithClientResource;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import scala.collection.immutable.HashMap;
import scala.concurrent.duration.Duration;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.apache.flink.table.catalog.confluent.CatalogTestUtil.*;
import static org.junit.Assert.*;


public class ConfluentSchemaRegistryCatalogIntegrationTest {

    private static EmbeddedKafka$ kafkaOps;
    private static EmbeddedKafkaConfig kafkaConfig;

    @ClassRule
    public static MiniClusterWithClientResource flinkCluster =
            new MiniClusterWithClientResource(
                    new MiniClusterResourceConfiguration.Builder()
                            .setNumberSlotsPerTaskManager(2)
                            .setNumberTaskManagers(1)
                            .build());

    @BeforeClass
    public static void setup() throws RestClientException, IOException {
        kafkaConfig = EmbeddedKafkaConfig.defaultConfig();
        EmbeddedKafka.start(kafkaConfig);
        kafkaOps = EmbeddedKafka$.MODULE$;
        kafkaOps.createCustomTopic(table1,new HashMap<>(),2,1, kafkaConfig);
        kafkaOps.createCustomTopic(table2,new HashMap<>(),2,1, kafkaConfig);
        kafkaOps.createCustomTopic(table2_target,new HashMap<>(),2,1, kafkaConfig);
//        kafkaOps.publishToKafka(table1,table1Message.,);
//        String a = kafkaOps.consumeFirstStringMessageFrom(table2,false, Duration.fromNanos(9000000000.0D),kafkaConfig);

        SchemaRegistryClientFactory schemaRegistryClientFactory = new SchemaRegistryClientFactory();
        SchemaRegistryClient schemaRegistryClient = schemaRegistryClientFactory.get(SCHEMA_REGISTRY_URIS,1000,new java.util.HashMap<>());
        schemaRegistryClient.register(table1, table1AvroSchema);
        schemaRegistryClient.register(table2, table2JsonSchema);
        schemaRegistryClient.register(table2_target, table2JsonSchema);
    }

    @AfterClass
    public static void tearDown() {
        EmbeddedKafka.stop();
    }

    @Test
    public void test() {

        TableEnvironment tableEnv = TableEnvironment.create(EnvironmentSettings.inStreamingMode());

        Map<String, String> properties = new java.util.HashMap<>();
        properties.put("connector", "kafka");
        properties.put(ConfluentSchemaRegistryCatalogFactoryOptions.BOOTSTRAP_SERVERS.key(), "localhost:" + kafkaConfig.kafkaPort());
        properties.put(ConfluentSchemaRegistryCatalogFactoryOptions.SCHEMA_REGISTRY_URI.key(), String.join(", ", SCHEMA_REGISTRY_URIS));
        // Create a Confluent Schema Registry Catalog
        Catalog catalog = new ConfluentSchemaRegistryCatalog("kafka", properties);
        // Register the catalog
        tableEnv.registerCatalog(CATALOG_NAME, catalog);

        tableEnv.executeSql("INSERT INTO " + table2Identifier + "\n" +
                "SELECT name,age\n" +
                "FROM (VALUES\n" +
                "('Abcd',30))\n" +
                "AS t(name, age)");

        tableEnv.executeSql("INSERT INTO " + table2_targetIdentifier + "\n" +
                "SELECT *\n" +
                "FROM " + table2Identifier);
        String record1 = kafkaOps.consumeFirstStringMessageFrom(table2, false, Duration.create(30, TimeUnit.SECONDS), kafkaConfig);
        assertNotNull(record1);
        assertEquals(table2Message,record1);
        String record2 = kafkaOps.consumeFirstStringMessageFrom(table2_target, false, Duration.create(30, TimeUnit.SECONDS), kafkaConfig);
        assertNotNull(record2);
        assertEquals(table2Message,record2);
    }
}
