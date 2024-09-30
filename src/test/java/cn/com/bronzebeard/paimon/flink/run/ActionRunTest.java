package cn.com.bronzebeard.paimon.flink.run;

import cn.com.bronzebeard.paimon.flink.utils.MiniClusterWithClientExtension;
import cn.com.bronzebeard.paimon.flink.utils.RunUtils;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.streaming.api.environment.ExecutionCheckpointingOptions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.time.Duration;

import static org.apache.flink.configuration.HeartbeatManagerOptions.HEARTBEAT_TIMEOUT;

/**
 * @author sherhomhuang
 * @date 2024/09/14 17:26
 * @description
 */
public class ActionRunTest {
    static Configuration getConf() {
        Configuration defaultConf = new Configuration();
        defaultConf.set(ExecutionCheckpointingOptions.CHECKPOINTING_INTERVAL, Duration.ofSeconds(5));
        defaultConf.set(ExecutionCheckpointingOptions.CHECKPOINTING_TIMEOUT, Duration.ofHours(12));
        return defaultConf;
    }

    @RegisterExtension
    protected static final MiniClusterWithClientExtension MINI_CLUSTER_EXTENSION =
            new MiniClusterWithClientExtension((new MiniClusterResourceConfiguration.Builder())
                    .setNumberTaskManagers(1)
                    .setNumberSlotsPerTaskManager(16)
                    .setConfiguration(new Configuration().set(HEARTBEAT_TIMEOUT, (long) Integer.MAX_VALUE)
                            .set(ExecutionCheckpointingOptions.CHECKPOINTING_INTERVAL,
                                    Duration.ofMillis(5000))
                    )
                    .build(), getConf());


    @BeforeAll
    public static void init() {
        System.setProperty("HADOOP_USER_NAME", "hadoop");
    }


    @Test
    public void test01() {
        String path = "src/test/resources/pipeline/test_pipeline_conf.json";
        RunUtils.run(path);
    }


    @Test
    public void initialTest() {
        String path = "src/test/resources/pipeline/test_pipeline_initial_conf.json";
        RunUtils.run(path);
    }
}
