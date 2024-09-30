package cn.com.bronzebeard.paimon.flink.mysql.format;

import cn.com.bronzebeard.paimon.flink.common.util.Asset;
import cn.com.bronzebeard.paimon.flink.mysql.format.entity.DataPair;
import cn.com.bronzebeard.paimon.flink.mysql.format.entity.PipelineInfo;
import com.alibaba.fastjson.JSON;
import com.ververica.cdc.connectors.mysql.source.config.MySqlSourceOptions;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.paimon.flink.action.Action;
import org.apache.paimon.flink.action.ActionFactory;
import org.apache.paimon.flink.action.MultipleParameterToolAdapter;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * @author sherhomhuang
 * @date 2024/05/08 19:41
 * @description
 */
public class MySqlSyncDatabaseExtActionFactory implements ActionFactory {

    public static final String IDENTIFIER = "mysql_sync_database_ext";
    public static final String CONF_PATH = "conf_path";

    @Override
    public Optional<Action> create(MultipleParameterToolAdapter params) {
        String confPath = params.get(CONF_PATH);
        PipelineInfo pipelineInfo = loadPipelineInfo(confPath);
        return create(pipelineInfo);
    }

    public Optional<Action> create(PipelineInfo pipelineInfo) {
        Set<String> dbSet = pipelineInfo.getDataPairList().stream()
                .map(DataPair::getSrcDatabasePattern).collect(Collectors.toSet());
        Asset.isTrue(dbSet.size() == 1, "Not support multi database: %s", dbSet);
        pipelineInfo.getSourceConfig().put(MySqlSourceOptions.DATABASE_NAME.key(), dbSet.stream().findAny().get());
        pipelineInfo.getSourceConfig().put(MySqlSourceOptions.HOSTNAME.key(), pipelineInfo.getIp());
        pipelineInfo.getSourceConfig().put(MySqlSourceOptions.PORT.key(), pipelineInfo.getPort());
        pipelineInfo.getSourceConfig().put(MySqlSourceOptions.USERNAME.key(), pipelineInfo.getUsername());
        pipelineInfo.getSourceConfig().put(MySqlSourceOptions.PASSWORD.key(), pipelineInfo.getPwd());
        // TODO When setting the parameter scan.newly-added-table.enabled=true in version 2.4.2, the
        // Insert data inserted after the newly created table cannot be captured. When set to false,
        // the mysql cdc works normally.
        pipelineInfo.getSourceConfig().put("scan.newly-added-table.enabled", "false");
        Set<String> rgrDbSet = pipelineInfo.getDataPairList().stream()
                .map(DataPair::getTgtDatabase).collect(Collectors.toSet());
        Asset.isTrue(rgrDbSet.size() == 1, "Not support multi paimon database: %s", dbSet);
        return Optional.of(new MySqlSyncDatabaseExtAction(pipelineInfo));
    }

    private PipelineInfo loadPipelineInfo(String confPath) {
        InputStream in = null;
        try {
            URI uri = URI.create(confPath);
            Configuration config = new Configuration();
            FileSystem fs = FileSystem.get(uri, config);//构建FileSystem
            in = fs.open(new Path(uri));//读取文件
            String configStr = IOUtils.toString(in, "UTF-8");
            return JSON.parseObject(configStr, PipelineInfo.class);
        } catch (IOException e) {
            throw new RuntimeException(e);
        } finally {
            IOUtils.closeQuietly(in);
        }

    }

    @Override
    public void printHelp() {
        System.out.println(
                "Action \"mysql_sync_database\" creates a streaming job "
                        + "with a Flink MySQL CDC source and multiple Paimon table sinks "
                        + "to synchronize a whole MySQL database into one Paimon database.\n"
                        + "Only MySQL tables with primary keys will be considered. "
                        + "Newly created MySQL tables after the job starts will not be included.");
        System.out.println();

        System.out.println("Syntax:");
        System.out.println(
                "  mysql_sync_database_ext --conf_path <conf_path> ");
        System.out.println();
        System.out.println(
                "For a complete list of supported configurations, "
                        + "see https://ververica.github.io/flink-cdc-connectors/master/content/connectors/mysql-cdc.html#connector-options");
        System.out.println();

        System.out.println("Examples:");
        System.out.println(
                "  mysql_sync_database_ext \\\n"
                        + "    --conf_path hdfs:///path/to/config/conf01.json \n");
    }

    @Override
    public String identifier() {
        return IDENTIFIER;
    }
}
