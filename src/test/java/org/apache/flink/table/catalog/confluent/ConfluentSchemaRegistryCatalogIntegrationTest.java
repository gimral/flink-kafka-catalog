package org.apache.flink.table.catalog.confluent;

import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.github.embeddedkafka.schemaregistry.EmbeddedKafka;
import io.github.embeddedkafka.schemaregistry.EmbeddedKafka$;
import io.github.embeddedkafka.schemaregistry.EmbeddedKafkaConfigImpl;
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
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.apache.flink.table.catalog.confluent.CatalogTestUtil.*;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;


public class ConfluentSchemaRegistryCatalogIntegrationTest {

    private static EmbeddedKafka$ kafkaOps;
    private static EmbeddedKafkaConfigImpl kafkaConfig;
    private final static int KAFKA_PORT = 6001;
    private final static int ZK_PORT = 6002;
    private final static int SR_PORT = 6003;
    public final static List<String> EMBEDDED_SCHEMA_REGISTRY_URIS = Collections.singletonList("http://localhost:"+SR_PORT);

    @ClassRule
    public static MiniClusterWithClientResource flinkCluster =
            new MiniClusterWithClientResource(
                    new MiniClusterResourceConfiguration.Builder()
                            .setNumberSlotsPerTaskManager(2)
                            .setNumberTaskManagers(1)
                            .build());

    @BeforeClass
    public static void setup() throws RestClientException, IOException {
        kafkaConfig = new EmbeddedKafkaConfigImpl(KAFKA_PORT,ZK_PORT,SR_PORT,new HashMap<>(),new HashMap<>(),new HashMap<>(), new HashMap<>());
        EmbeddedKafka.start(kafkaConfig);
        kafkaOps = EmbeddedKafka$.MODULE$;
        kafkaOps.createCustomTopic(table1,new HashMap<>(),2,1, kafkaConfig);
        kafkaOps.createCustomTopic(table2,new HashMap<>(),2,1, kafkaConfig);
        kafkaOps.createCustomTopic(table2_target,new HashMap<>(),2,1, kafkaConfig);
//        kafkaOps.publishToKafka(table1,table1Message.,);
//        String a = kafkaOps.consumeFirstStringMessageFrom(table2,false, Duration.fromNanos(9000000000.0D),kafkaConfig);

        SchemaRegistryClientFactory schemaRegistryClientFactory = new SchemaRegistryClientFactory();
        SchemaRegistryClient schemaRegistryClient = schemaRegistryClientFactory.get(EMBEDDED_SCHEMA_REGISTRY_URIS,1000,new java.util.HashMap<>());
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
        tableEnv.getConfig()        // access high-level configuration
                .getConfiguration()   // set low-level key-value options
                .setString("table.exec.resource.default-parallelism", "1");

        Map<String, String> properties = new java.util.HashMap<>();
        properties.put("connector", "kafka");
        properties.put(ConfluentSchemaRegistryCatalogFactoryOptions.BOOTSTRAP_SERVERS.key(), "localhost:" + kafkaConfig.kafkaPort());
        properties.put(ConfluentSchemaRegistryCatalogFactoryOptions.SCHEMA_REGISTRY_URI.key(), String.join(", ", EMBEDDED_SCHEMA_REGISTRY_URIS));
        // Create a Confluent Schema Registry Catalog
        Catalog catalog = new ConfluentSchemaRegistryCatalog("kafka", properties);
        // Register the catalog
        tableEnv.registerCatalog(CATALOG_NAME, catalog);


        tableEnv.executeSql("INSERT INTO " + table2Identifier + "\n" +
                "SELECT name,age,TO_DATE('1985-01-01') as birthDate,TO_TIMESTAMP('2022-08-04 19:00:00') as createTime,ROW(latitude,longitude, city,fruits) as location\n" +
                "FROM(\n" +
                "SELECT *,ROW(city_name,country) as city,ARRAY[ROW('berry','tomato'),ROW('pome','apple')] as fruits\n" +
                "FROM (VALUES\n" +
                "        ('Abcd',30,24.3,25.4,'istanbul','turkiye'))\n" +
                "AS t(name, age,latitude,longitude,city_name,country))");

//        tableEnv.executeSql("INSERT INTO " + table2_targetIdentifier + "\n" +
//                "SELECT *\n" +
//                "FROM " + table2Identifier);
        tableEnv.executeSql("INSERT INTO " + table2_targetIdentifier + "(name,age,birthDate,createTime,location)\n" +
                "SELECT name,age,birthDate,createTime,location\n" +
                "FROM " + table2Identifier);

        String record1 = kafkaOps.consumeFirstStringMessageFrom(table2, false, Duration.create(120, TimeUnit.SECONDS), kafkaConfig);
        assertNotNull(record1);
        assertEquals(table2Message,record1);
        String record2 = kafkaOps.consumeFirstStringMessageFrom(table2_target, false, Duration.create(60, TimeUnit.SECONDS), kafkaConfig);
        assertNotNull(record2);
        assertEquals(table2Message,record2);
    }
}
