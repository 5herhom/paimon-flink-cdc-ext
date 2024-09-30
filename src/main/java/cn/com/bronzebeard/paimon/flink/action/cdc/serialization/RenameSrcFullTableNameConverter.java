package cn.com.bronzebeard.paimon.flink.action.cdc.serialization;

import io.debezium.connector.AbstractSourceInfo;
import org.apache.paimon.flink.action.cdc.CdcMetadataConverter;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DataTypes;

/**
 * Name of the database that contain the row.
 */
public class RenameSrcFullTableNameConverter implements CdcMetadataConverter {
    private static final long serialVersionUID = 1L;
    public static final String FULL_TABLE_NAME_PATTERN = "%s.%s";
    private String targetName;

    public RenameSrcFullTableNameConverter(String targetName) {
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
        return String.format(FULL_TABLE_NAME_PATTERN
                , source.get(AbstractSourceInfo.DATABASE_NAME_KEY).asText()
                , source.get(AbstractSourceInfo.TABLE_NAME_KEY).asText()
        );
    }

    @Override
    public DataType dataType() {
        return DataTypes.STRING().notNull();
    }

    @Override
    public String columnName() {
        return targetName;
    }
}
