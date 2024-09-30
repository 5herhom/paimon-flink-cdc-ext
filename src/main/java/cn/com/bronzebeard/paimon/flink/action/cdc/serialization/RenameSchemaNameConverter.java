package cn.com.bronzebeard.paimon.flink.action.cdc.serialization;

import io.debezium.connector.AbstractSourceInfo;
import org.apache.paimon.flink.action.cdc.CdcMetadataConverter;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DataTypes;

/**
 * Name of the schema that contain the row.
 */
public class RenameSchemaNameConverter implements CdcMetadataConverter {
    private static final long serialVersionUID = 1L;
    private String targetName;
    public RenameSchemaNameConverter(String targetName) {
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
        return source.get(AbstractSourceInfo.SCHEMA_NAME_KEY).asText();
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
