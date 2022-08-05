package org.apache.flink.table.catalog.confluent.factories;

import org.apache.kafka.clients.admin.AdminClient;

import java.util.Map;

public class KafkaAdminClientFactory {
    public AdminClient get(Map<String, Object> kafkaProperties){
        return AdminClient.create(kafkaProperties);
    }
}
