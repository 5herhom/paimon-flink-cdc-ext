package cn.com.bronzebeard.paimon.flink.sink.cdc;

import cn.com.bronzebeard.paimon.flink.common.exception.GlobalException;
import cn.com.bronzebeard.paimon.flink.mysql.format.PatternMatchedToTargetMapper;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.flink.FlinkConnectorOptions;
import org.apache.paimon.flink.action.MultiTablesSinkMode;
import org.apache.paimon.flink.sink.cdc.*;
import org.apache.paimon.flink.utils.SingleOutputStreamOperatorUtils;
import org.apache.paimon.options.MemorySize;
import org.apache.paimon.options.Options;
import org.apache.paimon.schema.SchemaManager;
import org.apache.paimon.table.BucketMode;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.utils.Preconditions;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.apache.paimon.flink.action.MultiTablesSinkMode.COMBINED;
import static org.apache.paimon.flink.sink.FlinkStreamPartitioner.partition;

/**
 * @author sherhomhuang
 * @date 2024/05/13 10:22
 * @description
 */
public class FlinkCdcSyncDatabaseSinkExtBuilder<T> {
    private DataStream<T> input = null;
    private EventParser.Factory<T> parserFactory = null;
    private List<FileStoreTable> tables = new ArrayList<>();
    private boolean acceptNewTable = true;
    @Nullable
    private Integer parallelism;
    private double committerCpu;
    @Nullable
    private MemorySize committerMemory;

    // Paimon catalog used to check and create tables. There will be two
    //     places where this catalog is used. 1) in processing function,
    //     it will check newly added tables and create the corresponding
    //     Paimon tables. 2) in multiplex sink where it is used to
    //     initialize different writers to multiple tables.
    private Catalog.Loader catalogLoader;
    // database to sync, currently only support single database
    private String database;
    private MultiTablesSinkMode mode;
    private Map<String, List<Identifier>> patternToTgtIdMapper;

    private Set<String> initMonitoredSrcDbTbls;

    public FlinkCdcSyncDatabaseSinkExtBuilder<T> withInput(DataStream<T> input) {
        this.input = input;
        return this;
    }

    public FlinkCdcSyncDatabaseSinkExtBuilder<T> withParserFactory(
            EventParser.Factory<T> parserFactory) {
        this.parserFactory = parserFactory;
        return this;
    }

    public FlinkCdcSyncDatabaseSinkExtBuilder<T> withTables(List<FileStoreTable> tables) {
        this.tables = tables;
        return this;
    }

    public FlinkCdcSyncDatabaseSinkExtBuilder<T> withInitMonitoredDbTbls(Set<String> initMonitoredDbTbls) {
        this.initMonitoredSrcDbTbls = initMonitoredDbTbls;
        return this;
    }

    public FlinkCdcSyncDatabaseSinkExtBuilder<T> withTableOptions(Map<String, String> options) {
        return withTableOptions(Options.fromMap(options));
    }

    public FlinkCdcSyncDatabaseSinkExtBuilder<T> withTableOptions(Options options) {
        this.parallelism = options.get(FlinkConnectorOptions.SINK_PARALLELISM);
        this.committerCpu = options.get(FlinkConnectorOptions.SINK_COMMITTER_CPU);
        this.committerMemory = options.get(FlinkConnectorOptions.SINK_COMMITTER_MEMORY);
        return this;
    }

    public FlinkCdcSyncDatabaseSinkExtBuilder<T> withDatabase(String database) {
        this.database = database;
        return this;
    }

    public FlinkCdcSyncDatabaseSinkExtBuilder<T> withCatalogLoader(Catalog.Loader catalogLoader) {
        this.catalogLoader = catalogLoader;
        return this;
    }

    public FlinkCdcSyncDatabaseSinkExtBuilder<T> withMode(MultiTablesSinkMode mode) {
        this.mode = mode;
        return this;
    }

    public FlinkCdcSyncDatabaseSinkExtBuilder<T> withAcceptNewTable(Boolean acceptNewTable) {
        this.acceptNewTable = acceptNewTable;
        return this;
    }

    public FlinkCdcSyncDatabaseSinkExtBuilder<T> withPatternToTgtIdMapper(Map<String, List<Identifier>> patternToTgtIdMapper) {
        this.patternToTgtIdMapper = patternToTgtIdMapper;
        return this;
    }

    public void build() {
        Preconditions.checkNotNull(input);
        Preconditions.checkNotNull(parserFactory);
        Preconditions.checkNotNull(database);
        Preconditions.checkNotNull(catalogLoader);

        if (mode == COMBINED) {
            buildCombinedCdcSink();
        } else {
            throw new GlobalException("Not support mode %s", mode);
//            buildDividedCdcSink();
        }
    }

    private void buildCombinedCdcSink() {
        CdcDynamicTableParsingProcessExtFunction<T> tCdcDynamicTableParsingProcessExtFunction = new CdcDynamicTableParsingProcessExtFunction<>(
                catalogLoader, parserFactory);
        PatternMatchedToTargetMapper<Identifier> referenceTableNameConverter = new PatternMatchedToTargetMapper<>(patternToTgtIdMapper);
        initMonitoredSrcDbTbls.forEach(referenceTableNameConverter::addNewMatchedToMapper);
        tCdcDynamicTableParsingProcessExtFunction.setReferenceTableNameConverter(referenceTableNameConverter);
        SingleOutputStreamOperator<Void> parsed =
                input.forward()
                        .process(
                                tCdcDynamicTableParsingProcessExtFunction)
                        .name("Side Output")
                        .setParallelism(input.getParallelism());

        // for newly-added tables, create a multiplexing operator that handles all their records
        //     and writes to multiple tables
        DataStream<CdcMultiplexRecord> newlyAddedTableStream =
                SingleOutputStreamOperatorUtils.getSideOutput(
                        parsed, CdcDynamicTableParsingProcessExtFunction.DYNAMIC_OUTPUT_TAG);
        // handles schema change for newly added tables
        SingleOutputStreamOperatorUtils.getSideOutput(
                        parsed,
                        CdcDynamicTableParsingProcessExtFunction.DYNAMIC_SCHEMA_CHANGE_OUTPUT_TAG)
                .process(new MultiTableUpdatedDataFieldsProcessFunction(catalogLoader))
                .name("Schema Evolution");

        DataStream<CdcMultiplexRecord> partitioned =
                partition(
                        newlyAddedTableStream,
                        new CdcMultiplexRecordChannelComputer(catalogLoader),
                        parallelism);

        FlinkCdcMultiTableSink sink =
                new FlinkCdcMultiTableSink(catalogLoader, committerCpu, committerMemory);
        sink.sinkFrom(partitioned);
    }

    private void buildForFixedBucket(FileStoreTable table, DataStream<CdcRecord> parsed) {
        DataStream<CdcRecord> partitioned =
                partition(parsed, new CdcRecordChannelComputer(table.schema()), parallelism);
        new CdcFixedBucketSink(table).sinkFrom(partitioned);
    }

    private void buildForUnawareBucket(FileStoreTable table, DataStream<CdcRecord> parsed) {
        new CdcUnawareBucketSink(table, parallelism).sinkFrom(parsed);
    }

    private void buildDividedCdcSink() {
        Preconditions.checkNotNull(tables);

        SingleOutputStreamOperator<Void> parsed =
                input.forward()
                        .process(new CdcMultiTableParsingProcessFunction<>(parserFactory))
                        .setParallelism(input.getParallelism());

        for (FileStoreTable table : tables) {
            DataStream<Void> schemaChangeProcessFunction =
                    SingleOutputStreamOperatorUtils.getSideOutput(
                                    parsed,
                                    CdcMultiTableParsingProcessFunction
                                            .createUpdatedDataFieldsOutputTag(table.name()))
                            .process(
                                    new UpdatedDataFieldsProcessFunction(
                                            new SchemaManager(table.fileIO(), table.location()),
                                            Identifier.create(database, table.name()),
                                            catalogLoader));
            schemaChangeProcessFunction.getTransformation().setParallelism(1);
            schemaChangeProcessFunction.getTransformation().setMaxParallelism(1);

            DataStream<CdcRecord> parsedForTable =
                    SingleOutputStreamOperatorUtils.getSideOutput(
                            parsed,
                            CdcMultiTableParsingProcessFunction.createRecordOutputTag(
                                    table.name()));

            BucketMode bucketMode = table.bucketMode();
            switch (bucketMode) {
                case FIXED:
                    buildForFixedBucket(table, parsedForTable);
                    break;
                case DYNAMIC:
                    new CdcDynamicBucketSink(table).build(parsedForTable, parallelism);
                    break;
                case UNAWARE:
                    buildForUnawareBucket(table, parsedForTable);
                    break;
                case GLOBAL_DYNAMIC:
                default:
                    throw new UnsupportedOperationException(
                            "Unsupported bucket mode: " + bucketMode);
            }
        }
    }

}
