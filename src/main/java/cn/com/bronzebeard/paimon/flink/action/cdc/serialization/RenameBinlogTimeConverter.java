package cn.com.bronzebeard.paimon.flink.action.cdc.serialization;

import cn.com.bronzebeard.paimon.flink.common.util.DateUtil;
import io.debezium.connector.AbstractSourceInfo;
import org.apache.paimon.flink.action.cdc.CdcMetadataConverter;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DataTypes;

/**
 * It indicates the time that the change was made in the database. If the record is read from
 * snapshot of the table instead of the binlog, the value is always 0.
 */
public class RenameBinlogTimeConverter implements CdcMetadataConverter {
    private static final long serialVersionUID = 1L;
    private String targetName;

    public RenameBinlogTimeConverter(String targetName) {
        this.targetName = targetName;
    }

    public String getTargetName() {
        return targetName;
    }

    public void setTargetName(String targetName) {
        this.targetName = targetName;
    }

    @Override
    public String read(JsonNode source) {
//        return DateTimeUtils.formatTimestamp(
//                Timestamp.fromEpochMillis(
//                        source.get(AbstractSourceInfo.TIMESTAMP_KEY).asLong()),
//                DateTimeUtils.LOCAL_TZ,
//                3);
        return DateUtil.timestamp2String(source.get(AbstractSourceInfo.TIMESTAMP_KEY).asLong(), DateUtil.DEFAULT_FORMAT_STRING);
    }

    @Override
    public DataType dataType() {
        return DataTypes.STRING();
    }

    @Override
    public String columnName() {
        return targetName;
    }
}
