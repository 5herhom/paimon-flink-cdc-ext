package cn.com.bronzebeard.paimon.flink.action.cdc.mysql;

import cn.com.bronzebeard.paimon.flink.utils.JDBCUtils;
import com.alibaba.fastjson.JSONArray;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;
import java.sql.Statement;

/**
 * @author sherhomhuang
 * @date 2024/05/09 15:07
 * @description
 */
public class MysqlContainerTest  extends MySqlActionITCaseBase{
    private static final Logger LOG = LoggerFactory.getLogger(MysqlContainerTest.class);

    @BeforeAll
    public static void startContainers() {
        MYSQL_CONTAINER.withSetupSQL("mysql/sync_database_setup.sql");
        start();
    }

    @Test
    public void test01() throws SQLException {
        try (Statement statement = getStatement()) {
            statement.executeUpdate("USE paimon_sync_database");

            statement.executeUpdate("INSERT INTO t1 VALUES (1, 'one')");
            statement.executeUpdate("INSERT INTO t2 VALUES (2, 'two', 20, 200)");
            statement.executeUpdate("INSERT INTO t1 VALUES (3, 'three')");
            statement.executeUpdate("INSERT INTO t2 VALUES (4, 'four', 40, 400)");
            statement.executeUpdate("INSERT INTO t3 VALUES (-1)");
            JSONArray jsonArray = JDBCUtils.executeQuery(statement, "select * from t1");
            LOG.info(JSONArray.toJSONString(jsonArray,true));
        }
    }
}
