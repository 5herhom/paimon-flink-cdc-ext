package cn.com.bronzebeard.paimon.flink;

import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.config.TableConfigOptions;
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.CatalogContext;
import org.apache.paimon.catalog.CatalogFactory;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.data.DataFormatTestUtil;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.flink.action.ActionBase;
import org.apache.paimon.flink.action.ActionFactory;
import org.apache.paimon.fs.Path;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.sink.StreamTableCommit;
import org.apache.paimon.table.sink.StreamTableWrite;
import org.apache.paimon.table.source.Split;
import org.apache.paimon.table.source.TableRead;
import org.apache.paimon.types.RowType;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.ThreadLocalRandom;

/**
 * @author sherhomhuang
 * @date 2024/09/14 15:37
 * @description
 */
public abstract class ActionITCaseBase extends AbstractTestBase {

    protected String warehouse;
    protected String database;
    protected String tableName;
    protected String commitUser;
    protected StreamTableWrite write;
    protected StreamTableCommit commit;
    protected Catalog catalog;
    private long incrementalIdentifier;

    @BeforeEach
    public void before() throws IOException {
        warehouse = getTempDirPath();
        database = "default";
        tableName = "test_table_" + UUID.randomUUID();
        commitUser = UUID.randomUUID().toString();
        incrementalIdentifier = 0;
        catalog = CatalogFactory.createCatalog(CatalogContext.create(new Path(warehouse)));
    }

    @AfterEach
    public void after() throws Exception {
        if (write != null) {
            write.close();
            write = null;
        }
        if (commit != null) {
            commit.close();
            commit = null;
        }
        catalog.close();
    }

    protected FileStoreTable createFileStoreTable(
            RowType rowType,
            List<String> partitionKeys,
            List<String> primaryKeys,
            Map<String, String> options)
            throws Exception {
        return createFileStoreTable(tableName, rowType, partitionKeys, primaryKeys, options);
    }

    protected FileStoreTable createFileStoreTable(
            String tableName,
            RowType rowType,
            List<String> partitionKeys,
            List<String> primaryKeys,
            Map<String, String> options)
            throws Exception {
        Identifier identifier = Identifier.create(database, tableName);
        catalog.createDatabase(database, true);
        Map<String, String> newOptions = new HashMap<>(options);
        if (!newOptions.containsKey("bucket")) {
            newOptions.put("bucket", "1");
        }
        catalog.createTable(
                identifier,
                new Schema(rowType.getFields(), partitionKeys, primaryKeys, newOptions, ""),
                false);
        return (FileStoreTable) catalog.getTable(identifier);
    }

    protected FileStoreTable getFileStoreTable(String tableName) throws Exception {
        Identifier identifier = Identifier.create(database, tableName);
        return (FileStoreTable) catalog.getTable(identifier);
    }

    protected GenericRow rowData(Object... values) {
        return GenericRow.of(values);
    }

    protected void writeData(GenericRow... data) throws Exception {
        for (GenericRow d : data) {
            write.write(d);
        }
        commit.commit(incrementalIdentifier, write.prepareCommit(true, incrementalIdentifier));
        incrementalIdentifier++;
    }

    protected List<String> getResult(TableRead read, List<Split> splits, RowType rowType)
            throws Exception {
        try (RecordReader<InternalRow> recordReader = read.createReader(splits)) {
            List<String> result = new ArrayList<>();
            recordReader.forEachRemaining(
                    row -> result.add(DataFormatTestUtil.internalRowToString(row, rowType)));
            return result;
        }
    }

    @Override
    protected TableEnvironmentBuilder tableEnvironmentBuilder() {
        return super.tableEnvironmentBuilder()
                .checkpointIntervalMs(500)
                .parallelism(ThreadLocalRandom.current().nextInt(2) + 1);
    }

    @Override
    protected StreamExecutionEnvironmentBuilder streamExecutionEnvironmentBuilder() {
        return super.streamExecutionEnvironmentBuilder()
                .checkpointIntervalMs(500)
                .parallelism(ThreadLocalRandom.current().nextInt(2) + 1);
    }

    protected <T extends ActionBase> T createAction(Class<T> clazz, List<String> args) {
        return createAction(clazz, args.toArray(new String[0]));
    }

    protected <T extends ActionBase> T createAction(Class<T> clazz, String... args) {
        if (ThreadLocalRandom.current().nextBoolean()) {
            confuseArgs(args, "_", "-");
        } else {
            confuseArgs(args, "-", "_");
        }
        return ActionFactory.createAction(args)
                .filter(clazz::isInstance)
                .map(clazz::cast)
                .orElseThrow(() -> new RuntimeException("Failed to create action"));
    }

    // to test compatibility with old usage
    private void confuseArgs(String[] args, String regex, String replacement) {
        args[0] = args[0].replaceAll(regex, replacement);
        for (int i = 1; i < args.length; i += 2) {
            String arg = args[i].substring(2);
            args[i] = "--" + arg.replaceAll(regex, replacement);
        }
    }

    protected void callProcedure(String procedureStatement) {
        // default execution mode
        callProcedure(procedureStatement, true, false);
    }

    protected void callProcedure(String procedureStatement, boolean isStreaming, boolean dmlSync) {
        TableEnvironment tEnv;
        if (isStreaming) {
            tEnv = tableEnvironmentBuilder().streamingMode().checkpointIntervalMs(500).build();
        } else {
            tEnv = tableEnvironmentBuilder().batchMode().build();
        }

        tEnv.getConfig().set(TableConfigOptions.TABLE_DML_SYNC, dmlSync);

        tEnv.executeSql(
                String.format(
                        "CREATE CATALOG PAIMON WITH ('type'='paimon', 'warehouse'='%s');",
                        warehouse));
        tEnv.useCatalog("PAIMON");

        tEnv.executeSql(procedureStatement);
    }
}