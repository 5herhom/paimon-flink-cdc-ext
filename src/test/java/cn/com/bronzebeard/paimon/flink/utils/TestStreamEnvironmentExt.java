package cn.com.bronzebeard.paimon.flink.utils;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.minicluster.MiniCluster;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironmentFactory;
import org.apache.flink.streaming.util.TestStreamEnvironment;

import java.net.URL;
import java.util.Collection;

/**
 * @author sherhomhuang
 * @date 2024/09/14 19:13
 * @description
 */
public class TestStreamEnvironmentExt extends TestStreamEnvironment {
    public TestStreamEnvironmentExt(MiniCluster miniCluster, Configuration config, int parallelism, Collection<Path> jarFiles, Collection<URL> classPaths) {
        super(miniCluster, config, parallelism, jarFiles, classPaths);
    }

    public TestStreamEnvironmentExt(MiniCluster miniCluster, int parallelism) {
        super(miniCluster, parallelism);
    }

    public static void setAsContext(
            final MiniCluster miniCluster,
            final int parallelism,
            final Collection<Path> jarFiles,
            final Collection<URL> classpaths
            , final Configuration defaultConfig) {

        StreamExecutionEnvironmentFactory factory =
                conf -> {
                    conf = conf == null || conf.toMap().size() == 0 ? defaultConfig : conf;
                    TestStreamEnvironment env =
                            new TestStreamEnvironment(
                                    miniCluster, conf, parallelism, jarFiles, classpaths);


                    env.configure(conf, env.getClass().getClassLoader());
                    return env;
                };

        initializeContextEnvironment(factory);
    }
}
