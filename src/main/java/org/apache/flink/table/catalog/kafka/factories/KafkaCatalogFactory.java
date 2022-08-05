package org.apache.flink.table.catalog.kafka.factories;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.catalog.Catalog;
import org.apache.flink.table.catalog.kafka.KafkaCatalog;
import org.apache.flink.table.factories.CatalogFactory;
import org.apache.flink.table.factories.FactoryUtil;

import java.util.*;

public class KafkaCatalogFactory implements CatalogFactory {

    public KafkaCatalogFactory() {
    }

    @Override
    public String factoryIdentifier() {
        return KafkaCatalogFactoryOptions.IDENTIFIER;
    }

    public Set<ConfigOption<?>> requiredOptions() {
        final Set<ConfigOption<?>> options = new HashSet<>();
        options.add(KafkaCatalogFactoryOptions.BOOTSTRAP_SERVERS);
        options.add(KafkaCatalogFactoryOptions.SCHEMA_REGISTRY_URI);
        return options;
    }

    public Set<ConfigOption<?>> optionalOptions() {
        return new HashSet<>();
    }


    @Override
    public Catalog createCatalog(Context context) {
        final FactoryUtil.CatalogFactoryHelper helper =
                FactoryUtil.createCatalogFactoryHelper(this, context);
        helper.validateExcept(KafkaCatalogFactoryOptions.KAFKA_PREFIX,
                KafkaCatalogFactoryOptions.SCHEMA_REGISTRY_PREFIX,
                KafkaCatalogFactoryOptions.SCAN_PREFIX,
                KafkaCatalogFactoryOptions.SINK_PREFIX);

        return new KafkaCatalog(
                context.getName(),
                ((Configuration)helper.getOptions()).toMap(),
                new KafkaAdminClientFactory());
    }

}
