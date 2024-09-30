package cn.com.bronzebeard.paimon.flink.common.util;

import cn.com.bronzebeard.paimon.flink.action.cdc.serialization.CdcDebeziumDeserializationSchema;
import cn.com.bronzebeard.paimon.flink.common.exception.GlobalException;
import cn.com.bronzebeard.paimon.flink.mysql.format.entity.DataPair;
import cn.com.bronzebeard.paimon.flink.mysql.format.entity.PipelineInfo;
import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.connectors.mysql.source.MySqlSourceBuilder;
import com.ververica.cdc.connectors.mysql.source.config.MySqlSourceOptions;
import com.ververica.cdc.connectors.mysql.source.offset.BinlogOffset;
import com.ververica.cdc.connectors.mysql.source.offset.BinlogOffsetBuilder;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.ververica.cdc.debezium.table.DebeziumOptions;
import com.ververica.cdc.debezium.utils.JdbcUrlUtils;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.kafka.connect.json.JsonConverterConfig;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.flink.action.cdc.CdcSourceRecord;
import org.apache.paimon.flink.action.cdc.TypeMapping;
import org.apache.paimon.flink.action.cdc.schema.JdbcSchemaUtils;
import cn.com.bronzebeard.paimon.flink.mysql.format.entity.schema.JdbcSchemasInfo;
import org.apache.paimon.schema.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.function.Function;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static org.apache.paimon.flink.action.cdc.TypeMapping.TypeMappingMode.TINYINT1_NOT_BOOL;
import static org.apache.paimon.flink.action.cdc.mysql.MySqlTypeUtils.toPaimonTypeVisitor;
import static org.apache.paimon.options.OptionsUtils.convertToPropertiesPrefixKey;

/**
 * @author sherhomhuang
 * @date 2024/05/11 15:35
 * @description
 */
public class MySqlActionExtUtils {

    private static final Logger LOG = LoggerFactory.getLogger(MySqlActionExtUtils.class);

    public static final ConfigOption<Boolean> SCAN_NEWLY_ADDED_TABLE_ENABLED =
            ConfigOptions.key("scan.newly-added-table.enabled")
                    .booleanType()
                    .defaultValue(true)
                    .withDescription(
                            "Whether capture the scan the newly added tables or not, by default is true.");

    static Connection getConnection(Configuration mySqlConfig, Map<String, String> jdbcProperties)
            throws Exception {
        String paramString = "";
        if (!jdbcProperties.isEmpty()) {
            paramString =
                    "?"
                            + jdbcProperties.entrySet().stream()
                            .map(e -> e.getKey() + "=" + e.getValue())
                            .collect(Collectors.joining("&"));
        }

        String url =
                String.format(
                        "jdbc:mysql://%s:%d%s",
                        mySqlConfig.get(MySqlSourceOptions.HOSTNAME),
                        mySqlConfig.get(MySqlSourceOptions.PORT),
                        paramString);

        LOG.info("Connect to MySQL server using url: {}", url);

        return DriverManager.getConnection(
                url,
                mySqlConfig.get(MySqlSourceOptions.USERNAME),
                mySqlConfig.get(MySqlSourceOptions.PASSWORD));
    }

    public static JdbcSchemasInfo getMySqlTableInfos(
            Configuration mySqlConfig,
            Function<String, List<String>> monitorTableMatchedPatterns,
            List<Identifier> excludedTables,
            TypeMapping typeMapping)
            throws Exception {
        Pattern databasePattern =
                Pattern.compile(mySqlConfig.get(MySqlSourceOptions.DATABASE_NAME));
        JdbcSchemasInfo mySqlSchemasInfo = new JdbcSchemasInfo();
        Map<String, String> jdbcProperties = getJdbcProperties(typeMapping, mySqlConfig);

        try (Connection conn = MySqlActionExtUtils.getConnection(mySqlConfig, jdbcProperties)) {
            DatabaseMetaData metaData = conn.getMetaData();
            try (ResultSet schemas = metaData.getCatalogs()) {
                while (schemas.next()) {
                    String databaseName = schemas.getString("TABLE_CAT");
                    Matcher databaseMatcher = databasePattern.matcher(databaseName);
                    if (!databaseMatcher.matches()) {
                        continue;
                    }
                    try (ResultSet tables =
                                 metaData.getTables(databaseName, null, "%", new String[]{"TABLE"})) {
                        while (tables.next()) {
                            String tableName = tables.getString("TABLE_NAME");
                            String tableComment = tables.getString("REMARKS");
                            Identifier identifier = Identifier.create(databaseName, tableName);
                            List<String> matchedPattern = monitorTableMatchedPatterns.apply(tableName);
                            if (matchedPattern.size() > 0) {
                                Schema schema =
                                        JdbcSchemaUtils.buildSchema(
                                                metaData,
                                                databaseName,
                                                tableName,
                                                tableComment,
                                                typeMapping,
                                                toPaimonTypeVisitor());
                                matchedPattern.forEach(pattern -> {
                                    mySqlSchemasInfo.addSchema(identifier, String.format(DataPair.FULL_REG_TABLE_NAME_PATTERN, databaseName, pattern), schema);

                                });
                            } else {
                                excludedTables.add(identifier);
                            }
                        }
                    }
                }
            }
        }
        return mySqlSchemasInfo;
    }

    public static MySqlSource<CdcSourceRecord> buildMySqlSource(PipelineInfo pipelineInfo, TypeMapping typeMapping) {
        Configuration mySqlConfig = Configuration.fromMap(pipelineInfo.getSourceConfig());
        MySqlSourceBuilder<CdcSourceRecord> sourceBuilder = MySqlSource.builder();

        sourceBuilder
                .hostname(pipelineInfo.getIp())
                .port(Integer.parseInt(pipelineInfo.getPort()))
                .username(pipelineInfo.getUsername())
                .password(pipelineInfo.getPwd())
                .databaseList(mySqlConfig.get(MySqlSourceOptions.DATABASE_NAME))
                .tableList(pipelineInfo.getDataPairList().stream().map(DataPair::getFullSrcTableNamePattern).collect(Collectors.toList())
                        .toArray(new String[]{}));

        mySqlConfig.getOptional(MySqlSourceOptions.SERVER_ID).ifPresent(sourceBuilder::serverId);
        mySqlConfig
                .getOptional(MySqlSourceOptions.SERVER_TIME_ZONE)
                .ifPresent(sourceBuilder::serverTimeZone);
        // MySQL CDC using increment snapshot, splitSize is used instead of fetchSize (as in JDBC
        // connector). splitSize is the number of records in each snapshot split.
        mySqlConfig
                .getOptional(MySqlSourceOptions.SCAN_INCREMENTAL_SNAPSHOT_CHUNK_SIZE)
                .ifPresent(sourceBuilder::splitSize);
        mySqlConfig
                .getOptional(MySqlSourceOptions.CONNECT_TIMEOUT)
                .ifPresent(sourceBuilder::connectTimeout);
        mySqlConfig
                .getOptional(MySqlSourceOptions.CONNECT_MAX_RETRIES)
                .ifPresent(sourceBuilder::connectMaxRetries);
        mySqlConfig
                .getOptional(MySqlSourceOptions.CONNECTION_POOL_SIZE)
                .ifPresent(sourceBuilder::connectionPoolSize);
        mySqlConfig
                .getOptional(MySqlSourceOptions.HEARTBEAT_INTERVAL)
                .ifPresent(sourceBuilder::heartbeatInterval);

        String startupMode = mySqlConfig.get(MySqlSourceOptions.SCAN_STARTUP_MODE);
        // see
        // https://github.com/ververica/flink-cdc-connectors/blob/master/flink-connector-mysql-cdc/src/main/java/com/ververica/cdc/connectors/mysql/table/MySqlTableSourceFactory.java#L196
        if ("initial".equalsIgnoreCase(startupMode)) {
            Asset.isTrue(pipelineInfo.getDataPairList().size() == 1, "Only support one dataPair in initial mode. Now has %s table.", pipelineInfo.getDataPairList().size());
            sourceBuilder.startupOptions(StartupOptions.initial());
        } else if ("earliest-offset".equalsIgnoreCase(startupMode)) {
            sourceBuilder.startupOptions(StartupOptions.earliest());
        } else if ("latest-offset".equalsIgnoreCase(startupMode)) {
            sourceBuilder.startupOptions(StartupOptions.latest());
        } else if ("specific-offset".equalsIgnoreCase(startupMode)) {
            BinlogOffsetBuilder offsetBuilder = BinlogOffset.builder();
            String file = mySqlConfig.get(MySqlSourceOptions.SCAN_STARTUP_SPECIFIC_OFFSET_FILE);
            Long pos = mySqlConfig.get(MySqlSourceOptions.SCAN_STARTUP_SPECIFIC_OFFSET_POS);
            if (file != null && pos != null) {
                offsetBuilder.setBinlogFilePosition(file, pos);
            }
            mySqlConfig
                    .getOptional(MySqlSourceOptions.SCAN_STARTUP_SPECIFIC_OFFSET_GTID_SET)
                    .ifPresent(offsetBuilder::setGtidSet);
            mySqlConfig
                    .getOptional(MySqlSourceOptions.SCAN_STARTUP_SPECIFIC_OFFSET_SKIP_EVENTS)
                    .ifPresent(offsetBuilder::setSkipEvents);
            mySqlConfig
                    .getOptional(MySqlSourceOptions.SCAN_STARTUP_SPECIFIC_OFFSET_SKIP_ROWS)
                    .ifPresent(offsetBuilder::setSkipRows);
            sourceBuilder.startupOptions(StartupOptions.specificOffset(offsetBuilder.build()));
        } else if ("timestamp".equalsIgnoreCase(startupMode)) {
            sourceBuilder.startupOptions(
                    StartupOptions.timestamp(
                            mySqlConfig.get(MySqlSourceOptions.SCAN_STARTUP_TIMESTAMP_MILLIS)));
        }

        Properties jdbcProperties = new Properties();
        jdbcProperties.putAll(getJdbcProperties(typeMapping, mySqlConfig));
        sourceBuilder.jdbcProperties(jdbcProperties);

        Properties debeziumProperties = new Properties();
        debeziumProperties.putAll(
                convertToPropertiesPrefixKey(
                        mySqlConfig.toMap(), DebeziumOptions.DEBEZIUM_OPTIONS_PREFIX));
        sourceBuilder.debeziumProperties(debeziumProperties);

        Map<String, Object> customConverterConfigs = new HashMap<>();
        customConverterConfigs.put(JsonConverterConfig.DECIMAL_FORMAT_CONFIG, "numeric");
//        JsonDebeziumDeserializationSchema schema =
//                new JsonDebeziumDeserializationSchema(true, customConverterConfigs);
        CdcDebeziumDeserializationSchema schema =
                new CdcDebeziumDeserializationSchema(true, customConverterConfigs);

        boolean scanNewlyAddedTables = mySqlConfig.get(SCAN_NEWLY_ADDED_TABLE_ENABLED);

        return sourceBuilder
                .deserializer(schema)
                .includeSchemaChanges(true)
                .scanNewlyAddedTableEnabled(scanNewlyAddedTables)
                .build();
    }

    // see
    // https://ververica.github.io/flink-cdc-connectors/master/content/connectors/mysql-cdc.html#connector-options
    // https://dev.mysql.com/doc/connectors/en/connector-j-reference-configuration-properties.html
    private static Map<String, String> getJdbcProperties(
            TypeMapping typeMapping, Configuration mySqlConfig) {
        Map<String, String> jdbcProperties =
                convertToPropertiesPrefixKey(mySqlConfig.toMap(), JdbcUrlUtils.PROPERTIES_PREFIX);

        if (typeMapping.containsMode(TINYINT1_NOT_BOOL)) {
            String tinyInt1isBit = jdbcProperties.get("tinyInt1isBit");
            if (tinyInt1isBit == null) {
                jdbcProperties.put("tinyInt1isBit", "false");
            } else if ("true".equals(jdbcProperties.get("tinyInt1isBit"))) {
                throw new IllegalArgumentException(
                        "Type mapping option 'tinyint1-not-bool' conflicts with jdbc properties 'jdbc.properties.tinyInt1isBit=true'. "
                                + "Option 'tinyint1-not-bool' is equal to 'jdbc.properties.tinyInt1isBit=false'.");
            }
        }

        return jdbcProperties;
    }

    public static void registerJdbcDriver() {
        try {
            Class.forName("com.mysql.cj.jdbc.Driver");
        } catch (ClassNotFoundException ex) {
            LOG.warn(
                    "Cannot find class com.mysql.cj.jdbc.Driver. Try to load class com.mysql.jdbc.Driver.");
            try {
                Class.forName("com.mysql.jdbc.Driver");
            } catch (Exception e) {
                throw new RuntimeException(
                        "No suitable driver found. Cannot find class com.mysql.cj.jdbc.Driver and com.mysql.jdbc.Driver.");
            }
        }
    }
}
