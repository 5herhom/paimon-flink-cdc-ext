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
import cn.com.bronzebeard.paimon.flink.mysql.format.entity.DataPair;
import cn.com.bronzebeard.paimon.flink.CdcActionITCaseBase;
import cn.com.bronzebeard.paimon.flink.mysql.format.constants.SourceType;
import cn.com.bronzebeard.paimon.flink.mysql.format.entity.DataPairBuilder;
import cn.com.bronzebeard.paimon.flink.mysql.format.entity.PipelineInfo;
import com.alibaba.fastjson.JSON;
import com.ververica.cdc.connectors.mysql.source.config.MySqlSourceOptions;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.MultipleParameterTool;
import org.apache.paimon.CoreOptions;
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.flink.action.MultipleParameterToolAdapter;
import org.apache.paimon.flink.action.cdc.mysql.MySqlSyncDatabaseAction;
import org.apache.paimon.options.CatalogOptions;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.ZoneId;
import java.util.*;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * IT cases for {@link MySqlSyncDatabaseAction}.
 */
public class MySqlSyncDatabaseExtActionITCaseNoDocker extends CdcActionITCaseBase {


    String tempDir = "temp/data/";
    String database = "temp";
    String mysqlIp = "172.21.87.55";
    String mysqlPort = "3306";
    String mysqlDb = "ods_test";
    String mysqlUser = "root";
    String mysqlPwd = "root";

    /*
     * 测试全量表和新表创建
     * */
    @Test
    public void fullTableTest() throws Exception {
        Schema paimonSchema = Schema.newBuilder()
                .column("k", DataTypes.INT().notNull())
                .column("v1", DataTypes.VARCHAR(10))
                .primaryKey("k")
                .option("bucket", "1")
                .build();
        FileStoreTable tall = createPaimonTable("tall", paimonSchema);
        FileStoreTable t1 = createPaimonTable("t1", paimonSchema);
        FileStoreTable t22 = createPaimonTable("t22", paimonSchema);
        FileStoreTable t2 = createPaimonTable("t2", paimonSchema);
        FileStoreTable ta = createPaimonTable("ta", paimonSchema);
        FileStoreTable tb = createPaimonTable("tb", paimonSchema);


        String mySqlDatabase = mysqlDb;

//        Map<String, String> mySqlConfig = getBasicMySqlConfig();
//        mySqlConfig.put("database-name", mySqlDatabase);
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

        try (Statement statement = getStatement()) {

            statement.executeUpdate("USE " + mySqlDatabase);
            dropAndCreateTable(statement, "t1");
            dropAndCreateTable(statement, "a");
            dropAndCreateTable(statement, "ta");
            dropAndCreateTable(statement, "t2");
            statement.executeUpdate("INSERT INTO t2 VALUES (3, 'Hi')");

            MySqlSyncDatabaseExtAction action = build(pipelineInfo);
            runActionWithDefaultEnv(action);
            statement.executeUpdate("INSERT INTO t1 VALUES (1, 'one')");
            statement.executeUpdate("INSERT INTO a VALUES (1, 'one')");

            // make sure the job steps into incremental phase
            RowType rowType = paimonSchema.rowType();
            List<String> primaryKeys = paimonSchema.primaryKeys();
            waitForResult(Collections.singletonList("+I[1, one]"), t1, rowType, primaryKeys);

            // create new tables at runtime
            // synchronized table: t2, t22


            dropAndCreateTable(statement, "t22");

            statement.executeUpdate("INSERT INTO t22 VALUES (4, 'Hello')");

            statement.executeUpdate("INSERT INTO ta VALUES (1, 'Apache')");
            dropAndCreateTable(statement, "t3");
            statement.executeUpdate("INSERT INTO t3 VALUES (5, 'Paimon')");
            dropAndCreateTable(statement, "tb");
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

    /*
     * 测试增量表和新表创建
     * */
    @Test
    public void dtTableTest() throws Exception {
        Schema paimonSchema = Schema.newBuilder()
                .column("k", DataTypes.INT().notNull())
                .column("v1", DataTypes.VARCHAR(10))
                .column("add_time", DataTypes.INT())
                .column("dt", DataTypes.STRING().notNull())
                .primaryKey("dt", "k")
                .partitionKeys("dt")
                .option("bucket", "1")
                .build();
        FileStoreTable tall = createPaimonTable("tall_part_i", paimonSchema);
        FileStoreTable t1 = createPaimonTable("t1_part_i", paimonSchema);
        FileStoreTable t22 = createPaimonTable("t22_part_i", paimonSchema);
        FileStoreTable t2 = createPaimonTable("t2_part_i", paimonSchema);
        FileStoreTable ta = createPaimonTable("ta_part_i", paimonSchema);
        FileStoreTable tb = createPaimonTable("tb_part_i", paimonSchema);
        List<String> dtColumns = Arrays.asList("add_time");

        String mySqlDatabase = mysqlDb;


        List<DataPair> dataPairList = new ArrayList<>();
        dataPairList.add(DataPairBuilder.aDataPair()
                .srcDatabasePattern(mySqlDatabase)
                .srcTablePattern("t1_d")
                .tgtDatabase(this.database)
                .tgtTable("t1_part_i")
                .dtColumns(dtColumns)
                .pkList(Collections.singletonList("k"))
                .build());
        dataPairList.add(DataPairBuilder.aDataPair()
                .srcDatabasePattern(mySqlDatabase)
                .srcTablePattern("ta_d")
                .tgtDatabase(this.database)
                .tgtTable("ta_part_i")
                .dtColumns(dtColumns)
                .pkList(Collections.singletonList("k"))
                .build());
        dataPairList.add(DataPairBuilder.aDataPair()
                .srcDatabasePattern(mySqlDatabase)
                .srcTablePattern("t(\\d+)_d")
                .tgtDatabase(this.database)
                .tgtTable("tall_part_i")
                .dtColumns(dtColumns)
                .pkList(Collections.singletonList("k"))
                .build());
        PipelineInfo pipelineInfo = buildTestPipeline(dataPairList);

        try (Statement statement = getStatement()) {

            statement.executeUpdate("USE " + mySqlDatabase);
            List<String> mysqlTableNames = Arrays.asList("t1_d", "a_d", "ta_d"
                    , "t2_d", "t22_d", "t3_d", "tb_d");
            dropTableList(statement, mysqlTableNames);

            dropAndCreateTableDt(statement, "t1_d");
            dropAndCreateTableDt(statement, "a_d");
            dropAndCreateTableDt(statement, "ta_d");
            dropAndCreateTableDt(statement, "t2_d");
            statement.executeUpdate("INSERT INTO t2_d VALUES (3, 'Hi',1726239466)");

            MySqlSyncDatabaseExtAction action = build(pipelineInfo);
            runActionWithDefaultEnv(action);
            statement.executeUpdate("INSERT INTO t1_d VALUES (1, 'one',1726239466)");
            statement.executeUpdate("INSERT INTO a_d VALUES (1, 'one',1726239466)");

            // make sure the job steps into incremental phase
            RowType rowType = paimonSchema.rowType();
            List<String> primaryKeys = paimonSchema.primaryKeys();
            waitForResult(Collections.singletonList("+I[1, one, 1726239466, 2024-09-13]"), t1, rowType, primaryKeys);

            // create new tables at runtime
            // synchronized table: t2, t22


            dropAndCreateTableDt(statement, "t22_d");

            statement.executeUpdate("INSERT INTO t22_d VALUES (4, 'Hello',1726239466)");

            statement.executeUpdate("INSERT INTO ta_d VALUES (1, 'Apache',1726239466)");
            dropAndCreateTableDt(statement, "t3_d");
            statement.executeUpdate("INSERT INTO t3_d VALUES (5, 'Paimon',1726239466)");
            dropAndCreateTableDt(statement, "tb_d");
            statement.executeUpdate("INSERT INTO tb_d VALUES (1, 'sherhom',1726239466)");

            statement.executeUpdate("INSERT INTO t1_d VALUES (2, 'two',1726239466)");

            // tall should has all data from t1, t2, t22, t3
            waitForResult(Arrays.asList("+I[1, one, 1726239466, 2024-09-13]", "+I[3, Hi, 1726239466, 2024-09-13]"
                            , "+I[4, Hello, 1726239466, 2024-09-13]"
                            , "+I[5, Paimon, 1726239466, 2024-09-13]", "+I[2, two, 1726239466, 2024-09-13]")
                    , tall, rowType, primaryKeys);

            // t1 only should has data from t1
            waitForResult(Arrays.asList("+I[1, one, 1726239466, 2024-09-13]", "+I[2, two, 1726239466, 2024-09-13]"), t1, rowType, primaryKeys);
            waitForResult(Collections.singletonList("+I[1, Apache, 1726239466, 2024-09-13]"), ta, rowType, primaryKeys);

            // not sync tb ,should be empty
            waitForResult(Collections.emptyList(), tb, rowType, primaryKeys);

        }

    }

    /*
     * 测试新增字段。
     *
     * */
    @Test
    public void addColumnableTest() throws Exception {

        Schema paimonSchema = Schema.newBuilder()
                .column("k", DataTypes.INT().notNull())
                .column("v1", DataTypes.VARCHAR(10))
                .column("add_time", DataTypes.INT())
                .column("dt", DataTypes.STRING().notNull())
                .primaryKey("dt", "k")
                .partitionKeys("dt")
                .option("bucket", "1")
                .build();
        FileStoreTable tall = createPaimonTable("tall_part_i", paimonSchema);
        FileStoreTable t1 = createPaimonTable("t1_part_i", paimonSchema);
        FileStoreTable t22 = createPaimonTable("t22_part_i", paimonSchema);
        FileStoreTable t2 = createPaimonTable("t2_part_i", paimonSchema);
        FileStoreTable ta = createPaimonTable("ta_part_i", paimonSchema);
        FileStoreTable tb = createPaimonTable("tb_part_i", paimonSchema);
        List<String> dtColumns = Arrays.asList("add_time");

        String mySqlDatabase = mysqlDb;

        List<DataPair> dataPairList = new ArrayList<>();
        dataPairList.add(DataPairBuilder.aDataPair()
                .srcDatabasePattern(mySqlDatabase)
                .srcTablePattern("t1_d")
                .tgtDatabase(this.database)
                .tgtTable("t1_part_i")
                .dtColumns(dtColumns)
                .pkList(Collections.singletonList("k"))
                .build());
        dataPairList.add(DataPairBuilder.aDataPair()
                .srcDatabasePattern(mySqlDatabase)
                .srcTablePattern("ta_d")
                .tgtDatabase(this.database)
                .tgtTable("ta_part_i")
                .dtColumns(dtColumns)
                .pkList(Collections.singletonList("k"))
                .build());
        dataPairList.add(DataPairBuilder.aDataPair()
                .srcDatabasePattern(mySqlDatabase)
                .srcTablePattern("t(\\d+)_d")
                .tgtDatabase(this.database)
                .tgtTable("tall_part_i")
                .dtColumns(dtColumns)
                .pkList(Collections.singletonList("k"))
                .build());
        PipelineInfo pipelineInfo = buildTestPipeline(dataPairList);

        try (Statement statement = getStatement()) {
            statement.executeUpdate("USE " + mySqlDatabase);
            List<String> mysqlTableNames = Arrays.asList("t1_d", "a_d", "ta_d"
                    , "t2_d", "t22_d", "t3_d", "tb_d");
            dropTableList(statement, mysqlTableNames);

            dropAndCreateTableDt(statement, "t1_d");
            dropAndCreateTableDt(statement, "a_d");
            dropAndCreateTableDt(statement, "ta_d");
            dropAndCreateTableDt(statement, "t2_d");
            statement.executeUpdate("INSERT INTO t2_d VALUES (3, 'Hi',1726239466)");

            MySqlSyncDatabaseExtAction action = build(pipelineInfo);
            runActionWithDefaultEnv(action);
            statement.executeUpdate("INSERT INTO t1_d VALUES (1, 'one',1726239466)");
            statement.executeUpdate("INSERT INTO a_d VALUES (1, 'one',1726239466)");

            // make sure the job steps into incremental phase
            RowType rowType = paimonSchema.rowType();
            List<String> primaryKeys = paimonSchema.primaryKeys();
            waitForResult(Collections.singletonList("+I[1, one, 1726239466, 2024-09-13]"), t1, rowType, primaryKeys);

            // create new tables at runtime
            // synchronized table: t2, t22


            dropAndCreateTableDt(statement, "t22_d");

            statement.executeUpdate("INSERT INTO t22_d VALUES (4, 'Hello',1726239466)");
            statement.executeUpdate("ALTER TABLE t22_d ADD COLUMN v3 VARCHAR(10)");
            statement.executeUpdate("INSERT INTO t22_d VALUES (6, 'Hello',1726239466,'Hello2')");

            statement.executeUpdate("INSERT INTO ta_d VALUES (1, 'Apache',1726239466)");
            dropAndCreateTableDt(statement, "t3_d");
            statement.executeUpdate("INSERT INTO t3_d VALUES (5, 'Paimon',1726239466)");
            dropAndCreateTableDt(statement, "tb_d");
            statement.executeUpdate("INSERT INTO tb_d VALUES (1, 'sherhom',1726239466)");

            statement.executeUpdate("INSERT INTO t1_d VALUES (2, 'two',1726239466)");

            RowType newRowType = paimonSchema.rowType().appendDataField("v3", DataTypes.VARCHAR(10));
            // tall should has all data from t1, t2, t22, t3
            waitForResult(Arrays.asList("+I[1, one, 1726239466, 2024-09-13, NULL]", "+I[3, Hi, 1726239466, 2024-09-13, NULL]"
                            , "+I[4, Hello, 1726239466, 2024-09-13, NULL]", "+I[6, Hello, 1726239466, 2024-09-13, Hello2]"
                            , "+I[5, Paimon, 1726239466, 2024-09-13, NULL]", "+I[2, two, 1726239466, 2024-09-13, NULL]")
                    , tall, newRowType, primaryKeys);

            // t1 only should has data from t1
            waitForResult(Arrays.asList("+I[1, one, 1726239466, 2024-09-13]", "+I[2, two, 1726239466, 2024-09-13]"), t1, rowType, primaryKeys);
            waitForResult(Collections.singletonList("+I[1, Apache, 1726239466, 2024-09-13]"), ta, rowType, primaryKeys);

            // not sync tb ,should be empty
            waitForResult(Collections.emptyList(), tb, rowType, primaryKeys);

            statement.executeUpdate("ALTER TABLE t1_d ADD COLUMN v3 VARCHAR(10),ADD COLUMN v4 VARCHAR(10)");
            statement.executeUpdate("INSERT INTO t1_d VALUES (7, 'T1Hello',1726239466,'T1Hello3','T1Hello4')");
            newRowType = newRowType.appendDataField("v4", DataTypes.VARCHAR(10));
            waitForResult(Arrays.asList("+I[1, one, 1726239466, 2024-09-13, NULL, NULL]", "+I[3, Hi, 1726239466, 2024-09-13, NULL, NULL]"
                            , "+I[4, Hello, 1726239466, 2024-09-13, NULL, NULL]", "+I[6, Hello, 1726239466, 2024-09-13, Hello2, NULL]"
                            , "+I[5, Paimon, 1726239466, 2024-09-13, NULL, NULL]", "+I[2, two, 1726239466, 2024-09-13, NULL, NULL]"
                            , "+I[7, T1Hello, 1726239466, 2024-09-13, T1Hello3, T1Hello4]")
                    , tall, newRowType, primaryKeys);
            waitForResult(Arrays.asList("+I[1, one, 1726239466, 2024-09-13, NULL, NULL]", "+I[2, two, 1726239466, 2024-09-13, NULL, NULL]"
                            , "+I[7, T1Hello, 1726239466, 2024-09-13, T1Hello3, T1Hello4]")
                    , t1, newRowType, primaryKeys);


        }
    }

    @Test
    public void printPipeline() {
        List<String> dtColumns = Arrays.asList("add_time");

        String mySqlDatabase = mysqlDb;

        List<DataPair> dataPairList = new ArrayList<>();
        dataPairList.add(DataPairBuilder.aDataPair()
                .srcDatabasePattern(mySqlDatabase)
                .srcTablePattern("test_d_(\\d+)$")
                .tgtDatabase(this.database)
                .tgtTable("test_d_part_i")
                .dtColumns(dtColumns)
                .pkList(Collections.singletonList("k"))
                .build());
        dataPairList.add(DataPairBuilder.aDataPair()
                .srcDatabasePattern(mySqlDatabase)
                .srcTablePattern("test_dim_1")
                .tgtDatabase(this.database)
                .tgtTable("test_dim_1_part_f")
                .pkList(Collections.singletonList("k"))
                .build());
        dataPairList.add(DataPairBuilder.aDataPair()
                .srcDatabasePattern(mySqlDatabase)
                .srcTablePattern("test_dim_(\\d+)$")
                .tgtDatabase(this.database)
                .tgtTable("test_dim_part_f")
                .pkList(Collections.singletonList("k"))
                .build());
        dataPairList.add(DataPairBuilder.aDataPair()
                .srcDatabasePattern(mySqlDatabase)
                .srcTablePattern("test_dim_(\\d+)$")
                .tgtDatabase(this.database)
                .tgtTable("test_dim_part_i")
                .dtColumns(dtColumns)
                .pkList(Collections.singletonList("k"))
                .build());
        PipelineInfo pipelineInfo = buildTestPipeline(dataPairList);
        System.out.println(JSON.toJSONString(pipelineInfo, true));
    }

    protected void dropAndCreateTable(Statement statement, String tableName) throws SQLException {
        statement.executeUpdate("drop  table if exists " + tableName);
        statement.executeUpdate("CREATE TABLE " + tableName + " (k INT, v1 VARCHAR(10), PRIMARY KEY (k))");
    }

    protected void dropTableList(Statement statement, List<String> tableNameList) throws SQLException {
        tableNameList.forEach(tableName -> {
            try {
                statement.executeUpdate("drop  table if exists " + tableName);
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        });
    }

    protected void dropAndCreateTableDt(Statement statement, String tableName) throws SQLException {
        statement.executeUpdate("drop  table if exists " + tableName);
        statement.executeUpdate("CREATE TABLE " + tableName + " (k INT, v1 VARCHAR(10),add_time INT, PRIMARY KEY (k))");
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

    protected Statement getStatement() throws SQLException {
        Connection conn =
                DriverManager.getConnection(
                        String.format("jdbc:mysql://%s:%s/%s?characterEncoding=utf-8&useSSL=false"
                                , mysqlIp, mysqlPort, mysqlDb),
                        mysqlUser,
                        mysqlPwd);
        return conn.createStatement();
    }

    @Override
    protected String getTempDirPath(String dirName) {
        return tempDir;
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
        Map<String, String> catalogConfig = new HashMap<>();
        catalogConfig.put(CatalogOptions.WAREHOUSE.key(), tempDir);
        PipelineInfo pipelineInfo = new PipelineInfo();
        pipelineInfo.setIp(mysqlIp);
        pipelineInfo.setPort(mysqlPort);
        pipelineInfo.setUsername(mysqlUser);
        pipelineInfo.setPwd(mysqlPwd);
        pipelineInfo.setCatalogConfig(catalogConfig);
        pipelineInfo.setSourceTypeCode(SourceType.MYSQL.getCode());
        pipelineInfo.setSourceConfig(basicMySqlConfig);
        pipelineInfo.setDataPairList(dataPairList);
        return pipelineInfo;
    }

    protected Map<String, String> getBasicMySqlConfig() {
        Map<String, String> config = new HashMap<>();
//        config.put("hostname", mysqlIp);
//        config.put("port", mysqlPort);
//        config.put("username", mysqlUser);
//        config.put("password", mysqlPwd);
        // see mysql/my.cnf in test resources
        config.put("server-time-zone", ZoneId.of("Asia/Shanghai").toString());

        // TODO When setting the parameter scan.newly-added-table.enabled=true in version 2.4.2, the
        // Insert data inserted after the newly created table cannot be captured. When set to false,
        // the mysql cdc works normally.
//        config.put("scan.newly-added-table.enabled", "false");
        config.put(MySqlSourceOptions.SCAN_STARTUP_MODE.key(), "timestamp");
        config.put(MySqlSourceOptions.SCAN_STARTUP_TIMESTAMP_MILLIS.key(), String.valueOf(System.currentTimeMillis() - 30000));
        config.put("heartbeat.interval", "6000000");
//        config.put("debezium.heartbeat.interval.ms","6000000");
        return config;
    }

    public FileStoreTable createPaimonTable(String tableName, Schema paimonSchema) {
        return createPaimonTable(this.database, tableName, paimonSchema);
    }

    public FileStoreTable createPaimonTable(String db, String tableName, Schema paimonSchema) {
        try {
            Identifier identifier = Identifier.create(db, tableName);
            catalog.createDatabase(db, true);
            catalog.dropTable(identifier, true);
            catalog.createTable(identifier, paimonSchema, false);
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

}
