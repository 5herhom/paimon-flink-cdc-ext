package cn.com.bronzebeard.paimon.flink.common.constants;

import cn.com.bronzebeard.paimon.flink.action.cdc.serialization.*;
import com.google.common.collect.ImmutableMap;
import org.apache.paimon.flink.action.cdc.CdcMetadataConverter;

import java.util.Map;
import java.util.function.Function;

/**
 * @author sherhomhuang
 * @date 2024/09/18 16:49
 * @description
 */
public class GlobalConstants {
    public static final Map<MetaColumnType, Function<String, CdcMetadataConverter>> metadataConvertersFactories =
            new ImmutableMap.Builder<MetaColumnType, Function<String, CdcMetadataConverter>>()
                    .put(MetaColumnType.DATABASE, targetName -> new RenameDatabaseNameConverter(targetName))
                    .put(MetaColumnType.TABLE, targetName -> new RenameTableNameConverter(targetName))
                    .put(MetaColumnType.SCHEMA, targetName -> new RenameSchemaNameConverter(targetName))
                    .put(MetaColumnType.BINLOG_TIME, targetName -> new RenameBinlogTimeConverter(targetName))
                    .put(MetaColumnType.FULL_TABLE_NAME, targetName -> new RenameSrcFullTableNameConverter(targetName))
                    .build();

    public static Function<String, CdcMetadataConverter> getMetadataConverterFactory(MetaColumnType metaColumnType) {
        return metadataConvertersFactories.get(metaColumnType);
    }
}
