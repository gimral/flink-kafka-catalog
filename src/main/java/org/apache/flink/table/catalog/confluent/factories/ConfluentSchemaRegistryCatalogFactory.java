package org.apache.flink.table.catalog.confluent.factories;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.catalog.Catalog;
import org.apache.flink.table.catalog.confluent.ConfluentSchemaRegistryCatalog;
import org.apache.flink.table.factories.CatalogFactory;
import org.apache.flink.table.factories.FactoryUtil;

import java.util.*;

public class ConfluentSchemaRegistryCatalogFactory implements CatalogFactory {

    public ConfluentSchemaRegistryCatalogFactory() {
    }

    public Set<ConfigOption<?>> requiredOptions() {
        final Set<ConfigOption<?>> options = new HashSet<>();
        options.add(ConfluentSchemaRegistryCatalogFactoryOptions.BOOTSTRAP_SERVERS);
        options.add(ConfluentSchemaRegistryCatalogFactoryOptions.SCHEMA_REGISTRY_URI);
        return options;
    }

    public Set<ConfigOption<?>> optionalOptions() {
        return new HashSet<>();
    }


    @Override
    public Catalog createCatalog(Context context) {
        final FactoryUtil.CatalogFactoryHelper helper =
                FactoryUtil.createCatalogFactoryHelper(this, context);
        helper.validateExcept(ConfluentSchemaRegistryCatalogFactoryOptions.KAFKA_PREFIX,
                ConfluentSchemaRegistryCatalogFactoryOptions.SCHEMA_REGISTRY_PREFIX,
                ConfluentSchemaRegistryCatalogFactoryOptions.SCAN_PREFIX,
                ConfluentSchemaRegistryCatalogFactoryOptions.SINK_PREFIX);

        return new ConfluentSchemaRegistryCatalog(
                context.getName(),
                ((Configuration)helper.getOptions()).toMap());
    }

}
