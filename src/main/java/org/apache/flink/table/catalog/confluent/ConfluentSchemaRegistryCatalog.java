package org.apache.flink.table.catalog.confluent;

import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaMetadata;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import org.apache.commons.lang3.NotImplementedException;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.flink.formats.avro.typeutils.AvroSchemaConverter;
import org.apache.flink.formats.json.JsonRowSchemaConverter;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.catalog.*;
import org.apache.flink.table.catalog.confluent.factories.ConfluentSchemaRegistryCatalogFactoryOptions;
import org.apache.flink.table.catalog.exceptions.*;
import org.apache.flink.table.catalog.stats.CatalogColumnStatistics;
import org.apache.flink.table.catalog.stats.CatalogTableStatistics;
import org.apache.flink.table.expressions.Expression;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.utils.DataTypeUtils;
import org.apache.flink.table.types.utils.TypeConversions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.StringUtils.isNullOrWhitespaceOnly;

public class ConfluentSchemaRegistryCatalog extends AbstractCatalog {

    public static final String DEFAULT_DB = "default";
    public static final int DEFAULT_CACHE_SIZE = 1000;

    private static final Logger LOG = LoggerFactory.getLogger(ConfluentSchemaRegistryCatalog.class);

    private final SchemaRegistryClient schemaRegistryClient;
    private final Map<String, String> properties;

    public ConfluentSchemaRegistryCatalog(String name, Map<String, String> properties) {
        this(name, DEFAULT_DB, properties);
    }

    public ConfluentSchemaRegistryCatalog(String name, String defaultDatabase, Map<String, String> properties) {
        super(name, defaultDatabase);
        this.properties = properties;

        Map<String, String> schemaRegistryProperties = properties.entrySet().stream()
                .filter(p -> p.getKey().startsWith(ConfluentSchemaRegistryCatalogFactoryOptions.SCHEMA_REGISTRY_PREFIX))
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
        String baseURLs = schemaRegistryProperties.get(ConfluentSchemaRegistryCatalogFactoryOptions.SCHEMA_REGISTRY_URI);
        schemaRegistryClient = new CachedSchemaRegistryClient(baseURLs,DEFAULT_CACHE_SIZE,schemaRegistryProperties);
        LOG.info("Created Confluent Schema Registry Catalog {}", name);
    }

    @Override
    public void open() throws CatalogException {

    }

    @Override
    public void close() throws CatalogException {

    }

    @Override
    public List<String> listDatabases() throws CatalogException {
        return Collections.singletonList(getDefaultDatabase());
    }

    @Override
    public CatalogDatabase getDatabase(String databaseName) throws DatabaseNotExistException, CatalogException {
        if (databaseName.equals(getDefaultDatabase())) {
            return new CatalogDatabaseImpl(new HashMap<>(), "");
        } else {
            throw new DatabaseNotExistException(getName(), databaseName);
        }
    }

    @Override
    public boolean databaseExists(String databaseName) throws CatalogException {
        return listDatabases().contains(databaseName);
    }

    @Override
    public void createDatabase(String databaseName, CatalogDatabase database, boolean ignoreIfExists) throws CatalogException {
        throw new UnsupportedOperationException("confluent Schema Registry only supports read operations");
    }

    @Override
    public void dropDatabase(String s, boolean b, boolean b1) throws CatalogException {
        throw new UnsupportedOperationException("confluent Schema Registry only supports read operations");
    }

    @Override
    public void alterDatabase(String s, CatalogDatabase catalogDatabase, boolean b) throws CatalogException {
        throw new UnsupportedOperationException("confluent Schema Registry only supports read operations");
    }

    @Override
    public List<String> listTables(String databaseName) throws DatabaseNotExistException, CatalogException {
        checkArgument(
                !isNullOrWhitespaceOnly(databaseName), "databaseName cannot be null or empty");
        if (!this.databaseExists(databaseName)) {
            throw new DatabaseNotExistException(getName(), databaseName);
        } else {
            try {
                return schemaRegistryClient.getAllSubjects().stream()
                        .filter(name -> !name.endsWith(":key"))
                        .distinct()
                        .collect(Collectors.toList());
            } catch (Exception e) {
                throw new CatalogException("Could not list tables", e);
            }
        }
    }

    @Override
    public List<String> listViews(String databaseName) throws CatalogException {
        return Collections.emptyList();
    }

    @Override
    public CatalogBaseTable getTable(ObjectPath tablePath) throws TableNotExistException, CatalogException {
        checkNotNull(tablePath, "tablePath cannot be null");
        String topic = tablePath.getObjectName();

        try {
            SchemaMetadata latestSchemaMetadata = schemaRegistryClient.getLatestSchemaMetadata(topic);
            ResolvedSchema resolvedSchema = getTableSchema(latestSchemaMetadata);
            return new CatalogTableImpl(TableSchema.fromResolvedSchema(resolvedSchema),
                    getTableProperties(topic,latestSchemaMetadata.getSchemaType()), "");
        } catch (Exception e) {
            LOG.error("Error while accessing table " + tablePath + " : " + ExceptionUtils.getStackTrace(e));
            throw new TableNotExistException(this.getName(), tablePath);
        }
    }

    private ResolvedSchema getTableSchema(SchemaMetadata schemaMetadata) {
        DataType dataType;
        switch (schemaMetadata.getSchemaType()){
            case "JSON":
                dataType = TypeConversions.fromLegacyInfoToDataType(JsonRowSchemaConverter.convert(schemaMetadata.getSchema()));
                break;
            case "AVRO":
                dataType = TypeConversions.fromLegacyInfoToDataType(AvroSchemaConverter.convertToTypeInfo(schemaMetadata.getSchema()));
                break;
            default:
                throw new NotImplementedException("Not supporting serialization format");
        }
        return DataTypeUtils.expandCompositeTypeToSchema(dataType);
    }

    protected Map<String, String> getTableProperties(String topic, String type) {
        Map<String, String> props = new HashMap<>();
        props.put("connector", "kafka");
        props.put("topic", topic);
        props.put("scan.startup.mode", "latest-offset");

        if("JSON".equalsIgnoreCase(type))
            props.put("format","json");
        else if("AVRO".equalsIgnoreCase(type))
            props.put("format", "avro");
        else
            throw new NotImplementedException("Not supporting format");

        properties.entrySet().stream().filter(p -> p.getKey().startsWith(ConfluentSchemaRegistryCatalogFactoryOptions.SCAN_PREFIX)
                        || p.getKey().startsWith(ConfluentSchemaRegistryCatalogFactoryOptions.SINK_PREFIX))
                .forEach(p -> props.put(p.getKey(),p.getValue()));

        properties.entrySet().stream().filter(p -> p.getKey().startsWith(ConfluentSchemaRegistryCatalogFactoryOptions.KAFKA_PREFIX))
                .forEach(p -> props.put(p.getKey().substring(ConfluentSchemaRegistryCatalogFactoryOptions.KAFKA_PREFIX.length()),p.getValue()));

        return props;
    }

    @Override
    public boolean tableExists(ObjectPath tablePath) throws CatalogException {
        //TODO: Cache schema results??
        //TODO: Schema Naming Strategy
        checkNotNull(tablePath, "tablePath cannot be null");
        try {
            String topic = tablePath.getObjectName();
            return schemaRegistryClient.getAllSubjectsByPrefix(topic).contains(topic);
        } catch (Exception e) {
            throw new CatalogException("Could not list table", e);
        }
    }

    @Override
    public void dropTable(ObjectPath tablePath, boolean ignoreIfNotExists) throws CatalogException {
        throw new UnsupportedOperationException("confluent Schema Registry only supports read operations");
    }

    @Override
    public void renameTable(ObjectPath tablePath, String newTableName, boolean ignoreIfNotExists) throws CatalogException {
        throw new UnsupportedOperationException("confluent Schema Registry only supports read operations");
    }

    @Override
    public void createTable(ObjectPath tablePath, CatalogBaseTable table, boolean ignoreIfExists) throws CatalogException {
        throw new UnsupportedOperationException("confluent Schema Registry only supports read operations");
    }

    @Override
    public void alterTable(ObjectPath tablePath, CatalogBaseTable newCatalogTable, boolean ignoreIfNotExists) throws CatalogException {
        throw new UnsupportedOperationException("confluent Schema Registry only supports read operations");
    }

    @Override
    public List<CatalogPartitionSpec> listPartitions(ObjectPath tablePath) throws CatalogException {
        return Collections.emptyList();
    }

    @Override
    public List<CatalogPartitionSpec> listPartitions(ObjectPath tablePath, CatalogPartitionSpec partitionSpec) throws CatalogException {
        return Collections.emptyList();
    }

    @Override
    public List<CatalogPartitionSpec> listPartitionsByFilter(ObjectPath tablePath, List<Expression> filters) throws CatalogException {
        return Collections.emptyList();
    }

    @Override
    public CatalogPartition getPartition(ObjectPath tablePath, CatalogPartitionSpec partitionSpec) throws PartitionNotExistException, CatalogException {
        throw new PartitionNotExistException(getName(), tablePath, partitionSpec);
    }

    @Override
    public boolean partitionExists(ObjectPath tablePath, CatalogPartitionSpec partitionSpec) throws CatalogException {
        return false;
    }

    @Override
    public void createPartition(ObjectPath tablePath, CatalogPartitionSpec partitionSpec, CatalogPartition partition, boolean ignoreIfNotExists) throws CatalogException {
        throw new UnsupportedOperationException("confluent Schema Registry only supports read operations");
    }

    @Override
    public void dropPartition(ObjectPath tablePath, CatalogPartitionSpec partitionSpec, boolean ignoreIfNotExists) throws CatalogException {
        throw new UnsupportedOperationException("confluent Schema Registry only supports read operations");
    }

    @Override
    public void alterPartition(ObjectPath tablePath, CatalogPartitionSpec partitionSpec, CatalogPartition partition, boolean ignoreIfNotExists) throws CatalogException {
        throw new UnsupportedOperationException("confluent Schema Registry only supports read operations");
    }

    @Override
    public List<String> listFunctions(String databaseName) throws CatalogException {
        return Collections.emptyList();
    }

    @Override
    public CatalogFunction getFunction(ObjectPath functionPath) throws FunctionNotExistException, CatalogException {
        throw new FunctionNotExistException(getName(), functionPath);
    }

    @Override
    public boolean functionExists(ObjectPath functionPath) throws CatalogException {
        return false;
    }

    @Override
    public void createFunction(ObjectPath functionPath, CatalogFunction function, boolean ignoreIfExists) throws CatalogException {
        throw new UnsupportedOperationException("confluent Schema Registry only supports read operations");
    }

    @Override
    public void alterFunction(ObjectPath functionPath, CatalogFunction function, boolean ignoreIfExists) throws CatalogException {
        throw new UnsupportedOperationException("confluent Schema Registry only supports read operations");
    }

    @Override
    public void dropFunction(ObjectPath functionPath, boolean ignoreIfExists) throws CatalogException {
        throw new UnsupportedOperationException("confluent Schema Registry only supports read operations");
    }

    @Override
    public CatalogTableStatistics getTableStatistics(ObjectPath tablePath) throws CatalogException {
        return CatalogTableStatistics.UNKNOWN;
    }

    @Override
    public CatalogColumnStatistics getTableColumnStatistics(ObjectPath tablePath) throws CatalogException {
        return CatalogColumnStatistics.UNKNOWN;
    }

    @Override
    public CatalogTableStatistics getPartitionStatistics(ObjectPath tablePath, CatalogPartitionSpec partitionSpec) throws CatalogException {
        return CatalogTableStatistics.UNKNOWN;
    }

    @Override
    public CatalogColumnStatistics getPartitionColumnStatistics(ObjectPath tablePath, CatalogPartitionSpec partitionSpec) throws CatalogException {
        return CatalogColumnStatistics.UNKNOWN;
    }

    @Override
    public void alterTableStatistics(ObjectPath tablePath, CatalogTableStatistics tableStatistics, boolean ignoreIfNotExists) throws CatalogException {
        throw new UnsupportedOperationException("confluent Schema Registry only supports read operations");
    }

    @Override
    public void alterTableColumnStatistics(ObjectPath tablePath, CatalogColumnStatistics columnStatistics, boolean ignoreIfNotExists) throws CatalogException {
        throw new UnsupportedOperationException("confluent Schema Registry only supports read operations");
    }

    @Override
    public void alterPartitionStatistics(ObjectPath tablePath, CatalogPartitionSpec partitionSpec, CatalogTableStatistics tableStatistics, boolean ignoreIfNotExists) throws CatalogException {
        throw new UnsupportedOperationException("confluent Schema Registry only supports read operations");
    }

    @Override
    public void alterPartitionColumnStatistics(ObjectPath objectPath, CatalogPartitionSpec partitionSpec, CatalogColumnStatistics columnStatistics, boolean ignoreIfNotExists) throws CatalogException {
        throw new UnsupportedOperationException("confluent Schema Registry only supports read operations");
    }
}
