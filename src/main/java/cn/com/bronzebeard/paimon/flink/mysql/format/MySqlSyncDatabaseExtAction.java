package cn.com.bronzebeard.paimon.flink.mysql.format;

import cn.com.bronzebeard.paimon.flink.common.constants.GlobalConstants;
import cn.com.bronzebeard.paimon.flink.common.constants.MetaColumnType;
import cn.com.bronzebeard.paimon.flink.common.expression.AutoDateFormatExpression;
import cn.com.bronzebeard.paimon.flink.common.util.Asset;
import cn.com.bronzebeard.paimon.flink.common.util.CdcActionCommonExtUtils;
import cn.com.bronzebeard.paimon.flink.common.util.DateUtil;
import cn.com.bronzebeard.paimon.flink.common.util.MySqlActionExtUtils;
import cn.com.bronzebeard.paimon.flink.mysql.format.entity.DataPair;
import cn.com.bronzebeard.paimon.flink.mysql.format.entity.ParseInfo;
import cn.com.bronzebeard.paimon.flink.mysql.format.entity.ParseInfoBuilder;
import cn.com.bronzebeard.paimon.flink.mysql.format.entity.schema.JdbcSchemasInfo;
import cn.com.bronzebeard.paimon.flink.mysql.format.entity.PipelineInfo;
import cn.com.bronzebeard.paimon.flink.mysql.format.entity.schema.ListMergedJdbcTableInfo;
import cn.com.bronzebeard.paimon.flink.sink.cdc.FlinkCdcSyncDatabaseSinkExtBuilder;
import cn.com.bronzebeard.paimon.flink.sink.cdc.RichCdcMultiplexRecordEventExtParser;
import com.google.common.collect.ImmutableSet;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.paimon.annotation.VisibleForTesting;
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.flink.action.MultiTablesSinkMode;
import org.apache.paimon.flink.action.cdc.*;
import org.apache.paimon.flink.sink.cdc.*;
import org.apache.paimon.options.CatalogOptions;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.utils.Preconditions;

import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.connectors.mysql.source.config.MySqlSourceOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static org.apache.paimon.flink.action.cdc.CdcActionCommonUtils.schemaCompatible;
import static org.apache.paimon.utils.Preconditions.checkArgument;

/**
 * @author sherhomhuang
 * @date 2024/05/08 20:51
 * @description
 */
public class MySqlSyncDatabaseExtAction extends SyncDatabaseActionBase {
    private static final Logger LOG = LoggerFactory.getLogger(MySqlSyncDatabaseExtAction.class);
    private boolean ignoreIncompatible = false;

    // for test purpose
    private final List<Identifier> monitoredTables = new ArrayList<>();
    private final List<Identifier> excludedTables = new ArrayList<>();
    private PipelineInfo pipelineInfo;
    private Map<String, List<DataPair>> srcRegPatternToDataPairs;
    private Map<String, List<Identifier>> patternToTgtIdMapper;
    private Boolean isCreateTable = false;
    private List<ComputedColumn> computedColumns;

    public MySqlSyncDatabaseExtAction(PipelineInfo pipelineInfo) {
        super(pipelineInfo.getCatalogConfig().get(CatalogOptions.WAREHOUSE.key())
                , pipelineInfo.getDataPairList().stream().map(DataPair::getTgtDatabase).findAny().get()
                , pipelineInfo.getCatalogConfig()
                , pipelineInfo.getSourceConfig(), pipelineInfo.getSourceType().toPaimonCdcSourceType());
        this.pipelineInfo = pipelineInfo;
        this.metadataConverters = buildMetadataConverterList(pipelineInfo.getMetaColumnTypeToTargetColName()).toArray(new CdcMetadataConverter[0]);
        withMode(MultiTablesSinkMode.COMBINED);

        withTypeMapping(new TypeMapping(new ImmutableSet.Builder<TypeMapping.TypeMappingMode>()
                .add(TypeMapping.TypeMappingMode.BIGINT_UNSIGNED_TO_BIGINT)
                .build()
        ));
    }

    public MySqlSyncDatabaseExtAction ignoreIncompatible(boolean ignoreIncompatible) {
        this.ignoreIncompatible = ignoreIncompatible;
        return this;
    }

    @Override
    protected void beforeBuildingSourceSink() throws Exception {
        srcRegPatternToDataPairs = pipelineInfo.getDataPairList().stream()
                .collect(Collectors.groupingBy(DataPair::getFullSrcTableNamePattern));
        patternToTgtIdMapper = srcRegPatternToDataPairs
                .entrySet().stream()
                .map(e -> Pair.of(e.getKey()
                        , e.getValue().stream()
                                .map(dp -> new Identifier(dp.getTgtDatabase(), dp.getTgtTable()))
                                .collect(Collectors.toList())))
                .collect(Collectors.toMap(Pair::getLeft, Pair::getRight));
        JdbcSchemasInfo mySqlSchemasInfo =
                MySqlActionExtUtils.getMySqlTableInfos(
                        cdcSourceConfig,
                        tableName ->
                                findMatchedPatternByTable(tableName),
                        excludedTables,
                        typeMapping);


        List<DataPair> dataPairList = this.pipelineInfo.getDataPairList();
        for (DataPair dataPair : dataPairList) {
            String parimonTableName = dataPair.getFullTgtTableName();
            String srcDbPattern = dataPair.getSrcDatabasePattern();
            String srcTblPattern = dataPair.getSrcTablePattern();
            ListMergedJdbcTableInfo mergedJdbcTableInfo = new ListMergedJdbcTableInfo(parimonTableName);

            for (JdbcSchemasInfo.JdbcSchemaInfo jdbcSchemaInfo : mySqlSchemasInfo.getSchemasInfo()) {
                Identifier identifier = jdbcSchemaInfo.identifier();
                String databaseName = identifier.getDatabaseName();
                String tableName = identifier.getObjectName();
                if (databaseName.matches(srcDbPattern) && tableName.matches(srcTblPattern)) {
                    mergedJdbcTableInfo.merge(identifier, jdbcSchemaInfo.schema());
                }
            }
            Asset.isTrue(mergedJdbcTableInfo.identifiers().size() > 0, "No matched src table found %s", dataPair.getFullSrcTableNamePattern());
            Schema jdbcSchema = mergedJdbcTableInfo.schema();

            String location = mergedJdbcTableInfo.location();

            List<Identifier> identifiers = mergedJdbcTableInfo.identifiers();
            Identifier identifier = Identifier.fromString(dataPair.getFullTgtTableName());
            this.computedColumns = new ArrayList<>();
            if (dataPair.isDt()) {
                ComputedColumn dtComputeCol = getDtComputedColumnByDataPair(dataPair);
                this.computedColumns.add(dtComputeCol);
            }
            Schema fromMySql =
                    CdcActionCommonExtUtils.buildPaimonSchema(
                            identifier.getFullName(),
                            dataPair.getTargetDtColumnNames(),
                            dataPair.getPkList(),
                            this.computedColumns,
                            tableConfig,
                            jdbcSchema,
                            metadataConverters,
                            caseSensitive,
                            true);
            FileStoreTable table;
            try {
                table = (FileStoreTable) catalog.getTable(identifier);
                checkTableValidate(table);
                table = table.copy(tableConfig);
                Supplier<String> errMsg =
                        incompatibleMessage(table.schema(), location, jdbcSchema, identifier);
                if (findMatchedPatternByTable(table.schema(), fromMySql, errMsg)) {
                    tables.add(table);
                    monitoredTables.addAll(identifiers);
                } else {
                    excludedTables.addAll(identifiers);
                }
            } catch (Catalog.TableNotExistException e) {
                if (isCreateTable) {
                    catalog.createTable(identifier, fromMySql, false);
                    table = (FileStoreTable) catalog.getTable(identifier);
                    tables.add(table);
                    monitoredTables.addAll(identifiers);
                } else {
                    throw e;
                }
            }
        }

        Preconditions.checkState(
                !monitoredTables.isEmpty(),
                "No tables to be synchronized. Possible cause is the schemas of all tables in specified "
                        + "MySQL database are not compatible with those of existed Paimon tables. Please check the log.");
    }

    private List<CdcMetadataConverter> buildMetadataConverterList(Map<MetaColumnType, String> metaColumnTypeToTargetColName) {
        List<CdcMetadataConverter> metadataConverterList = metaColumnTypeToTargetColName.entrySet().stream()
                .map(
                        e -> {
                            Function<String, CdcMetadataConverter> factory = GlobalConstants.getMetadataConverterFactory(e.getKey());
                            Asset.notNull(factory, "No metadata converter factory found for %s", e.getKey());
                            return factory.apply(e.getValue());
                        }
                ).collect(Collectors.toList());
        return metadataConverterList;
    }

    public ComputedColumn getDtComputedColumnByDataPair(DataPair dataPair) {
        Asset.isTrue(dataPair.getTargetDtColumnNames().size() <= 1, "Not support dt columns more than 1. %s", dataPair.getFullTgtTableName());
        Asset.isTrue(dataPair.getDtColumns().size() <= 1, "Not support dt columns more than 1. %s", dataPair.getFullTgtTableName());
        ComputedColumn dtComputeCol = new ComputedColumn(dataPair.getTargetDtColumnNames().get(0), new AutoDateFormatExpression(
                dataPair.getDtColumns().get(0), DateUtil.DEFAULT_DAY_FORMAT_STRING, "1970-01-01"));
        return dtComputeCol;
    }

    private void checkTableValidate(FileStoreTable table) {
        String bucket = table.schema().options().get("bucket");
        Asset.isTrue(bucket != null && Integer.parseInt(bucket) > 0, "Only support fix bucket table. Table:%s", table.name());
    }

    @Override
    protected FlatMapFunction<CdcSourceRecord, RichCdcMultiplexRecord> recordParse() {
        return syncJobHandler.provideRecordParser(
                caseSensitive, Collections.emptyList(), typeMapping, metadataConverters);
    }

    @Override
    protected MySqlSource<CdcSourceRecord> buildSource() {
        return MySqlActionExtUtils.buildMySqlSource(pipelineInfo,
                typeMapping);
    }


    @Override
    protected EventParser.Factory<RichCdcMultiplexRecord> buildEventParserFactory() {
        NewTableSchemaBuilder schemaBuilder =
                new NewTableSchemaBuilder(tableConfig, caseSensitive, metadataConverters);
        Pattern includingPattern = Pattern.compile(includingTables);
        Pattern excludingPattern =
                excludingTables == null ? null : Pattern.compile(excludingTables);
        TableNameConverter tableNameConverter =
                new TableNameConverter(caseSensitive, mergeShards, tablePrefix, tableSuffix);
        Map<String, List<ParseInfo>> expectPatternToTarget = pipelineInfo.getDataPairList()
                .stream().collect(Collectors.groupingBy(DataPair::getFullSrcTableNamePattern))
                .entrySet().stream().map(e -> Pair.of(e.getKey(), e.getValue().stream().map(dp -> {

                    return ParseInfoBuilder.aParseInfo()
                            .tgtDb(dp.getTgtDatabase())
                            .tgtTable(dp.getTgtTable())
                            .computedColumns(dp.isDt() ? Collections.singletonList(getDtComputedColumnByDataPair(dp)) : Collections.emptyList())
                            .metadataConverters(new CdcMetadataConverter[0])
                            .dt(dp.isDt())
                            .build();
                }).collect(Collectors.toList())))
                .collect(Collectors.toMap(Pair::getKey, Pair::getValue));
        PatternMatchedToTargetMapper<ParseInfo> mysqlParseInfoMapper = new PatternMatchedToTargetMapper<>(expectPatternToTarget);
        return () ->
                new RichCdcMultiplexRecordEventExtParser(
                        schemaBuilder, includingPattern, excludingPattern, tableNameConverter, mysqlParseInfoMapper);
    }

    @Override
    protected void buildSink(
            DataStream<RichCdcMultiplexRecord> input,
            EventParser.Factory<RichCdcMultiplexRecord> parserFactory) {
        new FlinkCdcSyncDatabaseSinkExtBuilder<RichCdcMultiplexRecord>()
                .withInput(input)
                .withParserFactory(parserFactory)
                .withCatalogLoader(catalogLoader())
                .withDatabase(database)
                .withTables(tables)
                .withInitMonitoredDbTbls(monitoredTables.stream().map(Identifier::getFullName).collect(Collectors.toSet()))
                .withMode(mode)
                .withTableOptions(tableConfig)
                .withAcceptNewTable(pipelineInfo.newSourceConfig().get(MySqlSourceOptions.SCAN_NEWLY_ADDED_TABLE_ENABLED))
                .withPatternToTgtIdMapper(patternToTgtIdMapper)
                .build();
    }

    private void logNonPkTables(List<Identifier> nonPkTables) {
        if (!nonPkTables.isEmpty()) {
            LOG.debug(
                    "Didn't find primary keys for tables '{}'. "
                            + "These tables won't be synchronized.",
                    nonPkTables.stream()
                            .map(Identifier::getFullName)
                            .collect(Collectors.joining(",")));
            excludedTables.addAll(nonPkTables);
        }
    }

    private List<String> findMatchedPatternByTable(
            String mySqlTableName) {
        List<DataPair> dataPairList = this.pipelineInfo.getDataPairList();
        Set<String> mysqlTablePatterns = dataPairList.stream().map(DataPair::getSrcTablePattern).collect(Collectors.toSet());
        List<String> matchedPatterns = mysqlTablePatterns.stream().filter(pattern -> mySqlTableName.matches(pattern)).collect(Collectors.toList());
        if (matchedPatterns.isEmpty()) {
            LOG.debug("Source table '{}' is excluded.", mySqlTableName);
        }
        return matchedPatterns;
    }

    private boolean findMatchedPatternByTable(
            TableSchema tableSchema, Schema mySqlSchema, Supplier<String> errMsg) {
        if (schemaCompatible(tableSchema, mySqlSchema.fields())) {
            return true;
        } else if (ignoreIncompatible) {
            LOG.warn(errMsg.get() + "This table will be ignored.");
            return false;
        } else {
            throw new IllegalArgumentException(
                    errMsg.get()
                            + "If you want to ignore the incompatible tables, please specify --ignore-incompatible to true.");
        }
    }

    private Supplier<String> incompatibleMessage(
            TableSchema paimonSchema, String jdbcLocation, Schema jdbcSchema, Identifier identifier) {
        return () ->
                String.format(
                        "Incompatible schema found.\n"
                                + "Paimon table is: %s, fields are: %s.\n"
                                + "MySQL table is: %s, fields are: %s.\n",
                        identifier.getFullName(),
                        paimonSchema.fields(),
                        jdbcLocation,
                        jdbcSchema.fields());
    }

    @VisibleForTesting
    public List<Identifier> monitoredTables() {
        return monitoredTables;
    }

    @VisibleForTesting
    public List<Identifier> excludedTables() {
        return excludedTables;
    }
}
