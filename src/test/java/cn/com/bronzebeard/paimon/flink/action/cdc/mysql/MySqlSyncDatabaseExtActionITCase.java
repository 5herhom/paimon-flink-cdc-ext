/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package cn.com.bronzebeard.paimon.flink.action.cdc.mysql;

import cn.com.bronzebeard.paimon.flink.mysql.format.MySqlSyncDatabaseExtAction;
import cn.com.bronzebeard.paimon.flink.mysql.format.MySqlSyncDatabaseExtActionFactory;
import cn.com.bronzebeard.paimon.flink.mysql.format.constants.SourceType;
import cn.com.bronzebeard.paimon.flink.mysql.format.entity.DataPair;
import cn.com.bronzebeard.paimon.flink.mysql.format.entity.DataPairBuilder;
import cn.com.bronzebeard.paimon.flink.mysql.format.entity.PipelineInfo;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.MultipleParameterTool;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.core.execution.SavepointFormatType;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.SavepointRestoreSettings;
import org.apache.flink.streaming.api.graph.StreamGraph;
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.flink.action.MultipleParameterToolAdapter;
import org.apache.paimon.flink.action.cdc.mysql.MySqlSyncDatabaseAction;
import org.apache.paimon.options.CatalogOptions;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

import java.sql.SQLException;
import java.sql.Statement;
import java.util.*;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Collectors;

import static org.apache.paimon.flink.action.MultiTablesSinkMode.COMBINED;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * IT cases for {@link MySqlSyncDatabaseAction}.
 */
public class MySqlSyncDatabaseExtActionITCase extends MySqlActionITCaseBase {

    @TempDir
    java.nio.file.Path tempDir;

    @BeforeAll
    public static void startContainers() {
        MYSQL_CONTAINER.withSetupSQL("mysql/sync_database_ext_setup.sql");
        start();
    }

    public MySqlSyncDatabaseExtAction build(String configPath) {
        MultipleParameterTool parameterTool = MultipleParameterTool.fromArgs(new String[]{"--conf_path", configPath});
        return (MySqlSyncDatabaseExtAction) new MySqlSyncDatabaseExtActionFactory().create(new MultipleParameterToolAdapter(parameterTool)).get();
    }

    public MySqlSyncDatabaseExtAction build(PipelineInfo pipeline) {
        return (MySqlSyncDatabaseExtAction) new MySqlSyncDatabaseExtActionFactory().create(pipeline).get();
    }

    public PipelineInfo buildTestPipeline(List<DataPair> dataPairList) {
        Map<String, String> basicMySqlConfig = getBasicMySqlConfig();
        Map<String,String> catalogConfig=new HashMap<>();
        catalogConfig.put(CatalogOptions.WAREHOUSE.key(),tempDir.toString());
        PipelineInfo pipelineInfo = new PipelineInfo();
        pipelineInfo.setIp(MYSQL_CONTAINER.getHost());
        pipelineInfo.setPort(String.valueOf(MYSQL_CONTAINER.getDatabasePort()));
        pipelineInfo.setUsername(MYSQL_CONTAINER.getUsername());
        pipelineInfo.setPwd(MYSQL_CONTAINER.getPassword());
        pipelineInfo.setCatalogConfig(catalogConfig);
        pipelineInfo.setSourceTypeCode(SourceType.MYSQL.getCode());
        pipelineInfo.setSourceConfig(basicMySqlConfig);
        pipelineInfo.setDataPairList(dataPairList);
        return pipelineInfo;
    }

    public FileStoreTable createPaimonTable(String tableName, Schema fromMySql) {
        return createPaimonTable(this.database, tableName, fromMySql);
    }

    public FileStoreTable createPaimonTable(String db, String tableName, Schema fromMySql) {
        try {
            catalog.createDatabase(db,true);
            catalog.createTable(Identifier.create(db, tableName), fromMySql, false);
            return getFileStoreTable(db, tableName);
        } catch (Catalog.TableAlreadyExistException e) {
            throw new RuntimeException(e);
        } catch (Catalog.DatabaseNotExistException e) {
            throw new RuntimeException(e);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    protected FileStoreTable getFileStoreTable(String databaseName, String tableName) throws Exception {
        Identifier identifier = Identifier.create(databaseName, tableName);
        return (FileStoreTable) this.catalog.getTable(identifier);
    }

    @Test
    @Timeout(120)
    public void testAddIgnoredTable() throws Exception {
        Schema paimonSchema = Schema.newBuilder()
                .column("k", DataTypes.INT().notNull())
                .column("v1", DataTypes.VARCHAR(10))
                .primaryKey("k")
                .build();
        FileStoreTable tall = createPaimonTable("tall", paimonSchema);
        FileStoreTable t1 = createPaimonTable("t1", paimonSchema);
        FileStoreTable t22 = createPaimonTable("t22", paimonSchema);
        FileStoreTable t2 = createPaimonTable("t2", paimonSchema);
        FileStoreTable ta = createPaimonTable("ta", paimonSchema);
        FileStoreTable tb = createPaimonTable("tb", paimonSchema);


        String mySqlDatabase = "paimon_sync_database_add_ignored_table";

        Map<String, String> mySqlConfig = getBasicMySqlConfig();
        mySqlConfig.put("database-name", mySqlDatabase);
        List<DataPair> dataPairList = new ArrayList<>();
        dataPairList.add(DataPairBuilder.aDataPair()
                        .srcDatabasePattern(mySqlDatabase)
                        .srcTablePattern("t1")
                        .tgtDatabase(this.database)
                        .tgtTable("t1")
                        .dtColumns(Collections.emptyList())
                        .pkList(Collections.singletonList("k"))
                        .build());
        dataPairList.add(DataPairBuilder.aDataPair()
                        .srcDatabasePattern(mySqlDatabase)
                        .srcTablePattern("ta")
                        .tgtDatabase(this.database)
                        .tgtTable("ta")
                        .dtColumns(Collections.emptyList())
                        .pkList(Collections.singletonList("k"))
                        .build());
        dataPairList.add(DataPairBuilder.aDataPair()
                .srcDatabasePattern(mySqlDatabase)
                .srcTablePattern("t(\\d+)$")
                .tgtDatabase(this.database)
                .tgtTable("tall")
                .dtColumns(Collections.emptyList())
                .pkList(Collections.singletonList("k"))
                .build());
        PipelineInfo pipelineInfo = buildTestPipeline(dataPairList);
        MySqlSyncDatabaseExtAction action = build(pipelineInfo);
        runActionWithDefaultEnv(action);

        try (Statement statement = getStatement()) {

            statement.executeUpdate("USE " + mySqlDatabase);
            statement.executeUpdate("INSERT INTO t1 VALUES (1, 'one')");
            statement.executeUpdate("INSERT INTO a VALUES (1, 'one')");

            // make sure the job steps into incremental phase
            RowType rowType = paimonSchema.rowType();
            List<String> primaryKeys = paimonSchema.primaryKeys();
            waitForResult(Collections.singletonList("+I[1, one]"), t1, rowType, primaryKeys);

            // create new tables at runtime
            // synchronized table: t2, t22
            statement.executeUpdate("CREATE TABLE t2 (k INT, v1 VARCHAR(10), PRIMARY KEY (k))");
            statement.executeUpdate("INSERT INTO t2 VALUES (3, 'Hi')");

            statement.executeUpdate("CREATE TABLE t22 LIKE t2");
            statement.executeUpdate("INSERT INTO t22 VALUES (4, 'Hello')");

            statement.executeUpdate("CREATE TABLE ta (k INT, v1 VARCHAR(10), PRIMARY KEY (k))");
            statement.executeUpdate("INSERT INTO ta VALUES (1, 'Apache')");
            statement.executeUpdate("CREATE TABLE t3 (k INT, v1 VARCHAR(10))");
            statement.executeUpdate("INSERT INTO t3 VALUES (5, 'Paimon')");
            statement.executeUpdate("CREATE TABLE tb (k INT, v1 VARCHAR(10), PRIMARY KEY (k))");
            statement.executeUpdate("INSERT INTO tb VALUES (1, 'sherhom')");

            statement.executeUpdate("INSERT INTO t1 VALUES (2, 'two')");

            // tall should has all data from t1, t2, t22, t3
            waitForResult(Arrays.asList("+I[1, one]", "+I[3, Hi]", "+I[4, Hello]", "+I[5, Paimon]", "+I[2, two]"), tall, rowType, primaryKeys);

            // t1 only should has data from t1
            waitForResult(Arrays.asList("+I[1, one]", "+I[2, two]"), t1, rowType, primaryKeys);
            waitForResult(Collections.singletonList("+I[1, Apache]"), ta, rowType, primaryKeys);

            // not sync tb ,should be empty
            waitForResult(Collections.emptyList(), tb, rowType, primaryKeys);

        }
    }

    public void testNewlyAddedTable(
            int numOfNewlyAddedTables,
            boolean testSavepointRecovery,
            boolean testSchemaChange,
            String databaseName)
            throws Exception {
        JobClient client =
                buildSyncDatabaseActionWithNewlyAddedTables(databaseName, testSchemaChange);
        waitJobRunning(client);

        try (Statement statement = getStatement()) {
            testNewlyAddedTableImpl(
                    client,
                    statement,
                    numOfNewlyAddedTables,
                    testSavepointRecovery,
                    testSchemaChange,
                    databaseName);
        }
    }

    private void testNewlyAddedTableImpl(
            JobClient client,
            Statement statement,
            int newlyAddedTableCount,
            boolean testSavepointRecovery,
            boolean testSchemaChange,
            String databaseName)
            throws Exception {
        FileStoreTable table1 = getFileStoreTable("t1");
        FileStoreTable table2 = getFileStoreTable("t2");

        statement.executeUpdate("USE " + databaseName);

        statement.executeUpdate("INSERT INTO t1 VALUES (1, 'one')");
        statement.executeUpdate("INSERT INTO t2 VALUES (2, 'two', 20, 200)");
        statement.executeUpdate("INSERT INTO t1 VALUES (3, 'three')");
        statement.executeUpdate("INSERT INTO t2 VALUES (4, 'four', 40, 400)");
        RowType rowType1 =
                RowType.of(
                        new DataType[]{DataTypes.INT().notNull(), DataTypes.VARCHAR(10)},
                        new String[]{"k", "v1"});
        List<String> primaryKeys1 = Collections.singletonList("k");
        List<String> expected = Arrays.asList("+I[1, one]", "+I[3, three]");
        waitForResult(expected, table1, rowType1, primaryKeys1);

        RowType rowType2 =
                RowType.of(
                        new DataType[]{
                                DataTypes.INT().notNull(),
                                DataTypes.VARCHAR(10).notNull(),
                                DataTypes.INT(),
                                DataTypes.BIGINT()
                        },
                        new String[]{"k1", "k2", "v1", "v2"});
        List<String> primaryKeys2 = Arrays.asList("k1", "k2");
        expected = Arrays.asList("+I[2, two, 20, 200]", "+I[4, four, 40, 400]");
        waitForResult(expected, table2, rowType2, primaryKeys2);

        // Create new tables at runtime. The Flink job is guaranteed to at incremental
        //    sync phase, because the newly added table will not be captured in snapshot
        //    phase.
        Map<String, List<Tuple2<Integer, String>>> recordsMap = new HashMap<>();
        List<String> newTablePrimaryKeys = Collections.singletonList("k");
        RowType newTableRowType =
                RowType.of(
                        new DataType[]{DataTypes.INT().notNull(), DataTypes.VARCHAR(10)},
                        new String[]{"k", "v1"});
        int newTableCount = 0;
        String newTableName = getNewTableName(newTableCount);

        createNewTable(statement, newTableName);
        statement.executeUpdate(
                String.format("INSERT INTO `%s`.`t2` VALUES (8, 'eight', 80, 800)", databaseName));
        List<Tuple2<Integer, String>> newTableRecords = getNewTableRecords();
        recordsMap.put(newTableName, newTableRecords);
        List<String> newTableExpected = getNewTableExpected(newTableRecords);
        insertRecordsIntoNewTable(statement, databaseName, newTableName, newTableRecords);

        // suspend the job and restart from savepoint
        if (testSavepointRecovery) {
            String savepoint =
                    client.stopWithSavepoint(
                                    false,
                                    tempDir.toUri().toString(),
                                    SavepointFormatType.CANONICAL)
                            .join();
            assertThat(savepoint).isNotBlank();

            client =
                    buildSyncDatabaseActionWithNewlyAddedTables(
                            savepoint, databaseName, testSchemaChange);
            waitJobRunning(client);
        }

        // wait until table t2 contains the updated record, and then check
        //     for existence of first newly added table
        expected =
                Arrays.asList(
                        "+I[2, two, 20, 200]", "+I[4, four, 40, 400]", "+I[8, eight, 80, 800]");
        waitForResult(expected, table2, rowType2, primaryKeys2);

        FileStoreTable newTable = getFileStoreTable(newTableName);
        waitForResult(newTableExpected, newTable, newTableRowType, newTablePrimaryKeys);

        for (newTableCount = 1; newTableCount < newlyAddedTableCount; ++newTableCount) {
            // create new table
            newTableName = getNewTableName(newTableCount);
            createNewTable(statement, newTableName);

            Thread.sleep(5000L);

            // insert records
            newTableRecords = getNewTableRecords();
            recordsMap.put(newTableName, newTableRecords);
            insertRecordsIntoNewTable(statement, databaseName, newTableName, newTableRecords);
            newTable = getFileStoreTable(newTableName);
            newTableExpected = getNewTableExpected(newTableRecords);
            waitForResult(newTableExpected, newTable, newTableRowType, newTablePrimaryKeys);
        }

        ThreadLocalRandom random = ThreadLocalRandom.current();

        // pick a random newly added table and insert records
        int pick = random.nextInt(newlyAddedTableCount);
        String tableName = getNewTableName(pick);
        List<Tuple2<Integer, String>> records = recordsMap.get(tableName);
        records.add(Tuple2.of(80, "eighty"));
        newTable = getFileStoreTable(tableName);
        newTableExpected = getNewTableExpected(records);
        statement.executeUpdate(
                String.format(
                        "INSERT INTO `%s`.`%s` VALUES (80, 'eighty')", databaseName, tableName));

        waitForResult(newTableExpected, newTable, newTableRowType, newTablePrimaryKeys);

        // test schema change
        if (testSchemaChange) {
            pick = random.nextInt(newlyAddedTableCount);
            tableName = getNewTableName(pick);
            records = recordsMap.get(tableName);

            statement.executeUpdate(
                    String.format(
                            "ALTER TABLE `%s`.`%s` ADD COLUMN v2 INT", databaseName, tableName));
            statement.executeUpdate(
                    String.format(
                            "INSERT INTO `%s`.`%s` VALUES (100, 'hundred', 10000)",
                            databaseName, tableName));

            List<String> expectedRecords =
                    records.stream()
                            .map(tuple -> String.format("+I[%d, %s, NULL]", tuple.f0, tuple.f1))
                            .collect(Collectors.toList());
            expectedRecords.add("+I[100, hundred, 10000]");

            newTable = getFileStoreTable(tableName);
            RowType rowType =
                    RowType.of(
                            new DataType[]{
                                    DataTypes.INT().notNull(), DataTypes.VARCHAR(10), DataTypes.INT()
                            },
                            new String[]{"k", "v1", "v2"});
            waitForResult(expectedRecords, newTable, rowType, newTablePrimaryKeys);

            // test that catalog loader works
            assertThat(getFileStoreTable(tableName).options())
                    .containsEntry("alter-table-test", "true");
        }
    }

    private List<String> getNewTableExpected(List<Tuple2<Integer, String>> newTableRecords) {
        return newTableRecords.stream()
                .map(tuple -> String.format("+I[%d, %s]", tuple.f0, tuple.f1))
                .collect(Collectors.toList());
    }

    private List<Tuple2<Integer, String>> getNewTableRecords() {
        List<Tuple2<Integer, String>> records = new LinkedList<>();
        int count = ThreadLocalRandom.current().nextInt(10) + 1;
        for (int i = 0; i < count; i++) {
            records.add(Tuple2.of(i, "varchar_" + i));
        }
        return records;
    }

    private void insertRecordsIntoNewTable(
            Statement statement,
            String databaseName,
            String newTableName,
            List<Tuple2<Integer, String>> newTableRecords)
            throws SQLException {
        String sql =
                String.format(
                        "INSERT INTO `%s`.`%s` VALUES %s",
                        databaseName,
                        newTableName,
                        newTableRecords.stream()
                                .map(tuple -> String.format("(%d, '%s')", tuple.f0, tuple.f1))
                                .collect(Collectors.joining(", ")));
        statement.executeUpdate(sql);
    }

    private String getNewTableName(int newTableCount) {
        return "t_new_table_" + newTableCount;
    }

    private void createNewTable(Statement statement, String newTableName) throws SQLException {
        statement.executeUpdate(
                String.format(
                        "CREATE TABLE %s (k INT, v1 VARCHAR(10), PRIMARY KEY (k))", newTableName));
    }

    private JobClient buildSyncDatabaseActionWithNewlyAddedTables(
            String databaseName, boolean testSchemaChange) throws Exception {
        return buildSyncDatabaseActionWithNewlyAddedTables(null, databaseName, testSchemaChange);
    }

    private JobClient buildSyncDatabaseActionWithNewlyAddedTables(
            String savepointPath, String databaseName, boolean testSchemaChange) throws Exception {

        Map<String, String> mySqlConfig = getBasicMySqlConfig();
        mySqlConfig.put("database-name", databaseName);
        mySqlConfig.put("scan.incremental.snapshot.chunk.size", "1");

        Map<String, String> catalogConfig =
                testSchemaChange
                        ? Collections.singletonMap(
                        CatalogOptions.METASTORE.key(), "test-alter-table")
                        : Collections.emptyMap();

        MySqlSyncDatabaseAction action =
                syncDatabaseActionBuilder(mySqlConfig)
                        .withCatalogConfig(catalogConfig)
                        .withTableConfig(getBasicTableConfig())
                        .includingTables("t.+")
                        .withMode(COMBINED.configString())
                        .build();
        action.withStreamExecutionEnvironment(env).build();

        if (Objects.nonNull(savepointPath)) {
            StreamGraph streamGraph = env.getStreamGraph();
            JobGraph jobGraph = streamGraph.getJobGraph();
            jobGraph.setSavepointRestoreSettings(
                    SavepointRestoreSettings.forPath(savepointPath, true));
            return env.executeAsync(streamGraph);
        }
        return env.executeAsync();
    }

    private class SyncNewTableJob implements Runnable {

        private final int ith;
        private final Statement statement;
        private final List<Tuple2<Integer, String>> records;

        SyncNewTableJob(int ith, Statement statement, List<Tuple2<Integer, String>> records) {
            this.ith = ith;
            this.statement = statement;
            this.records = records;
        }

        @Override
        public void run() {
            String newTableName = "t" + ith;
            try {
                createNewTable(statement, newTableName);
                String sql =
                        String.format(
                                "INSERT INTO %s VALUES %s",
                                newTableName,
                                records.stream()
                                        .map(
                                                tuple ->
                                                        String.format(
                                                                "(%d, '%s')", tuple.f0, tuple.f1))
                                        .collect(Collectors.joining(", ")));
                statement.executeUpdate(sql);
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        }
    }
}
