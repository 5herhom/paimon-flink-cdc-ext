package cn.com.bronzebeard.paimon.flink.utils;


import org.apache.paimon.flink.action.FlinkActions;

import java.util.concurrent.TimeUnit;

import static cn.com.bronzebeard.paimon.flink.mysql.format.MySqlSyncDatabaseExtActionFactory.IDENTIFIER;

/**
 * @author sherhomhuang
 * @date 2024/09/14 17:12
 * @description
 */
public class RunUtils {
    public static void run(String configPath) {
        String[] args = new String[]{IDENTIFIER,"--conf_path", configPath};
        try {
            FlinkActions.main(args);
            TimeUnit.SECONDS.sleep(12220000);
        } catch (Exception e) {
            try {
                TimeUnit.SECONDS.sleep(4);
            } catch (InterruptedException ex) {
                throw new RuntimeException(ex);
            }
            e.printStackTrace();
        }
    }

}
