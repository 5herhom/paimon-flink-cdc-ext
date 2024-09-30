package cn.com.bronzebeard.paimon.flink.mysql.format.entity.schema;

import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.flink.action.cdc.schema.JdbcSchemaUtils;
import org.apache.paimon.flink.action.cdc.schema.JdbcTableInfo;
import org.apache.paimon.schema.Schema;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import static org.apache.paimon.utils.Preconditions.checkArgument;
import static org.apache.paimon.utils.Preconditions.checkNotNull;

/**
 * @author sherhomhuang
 * @date 2024/09/09 18:58
 * @description
 */
public class ListMergedJdbcTableInfo implements JdbcTableInfo {
    private final List<Identifier> tables;

    private String tableName;
    private Schema schema;

    public ListMergedJdbcTableInfo(String tableName) {
        this.tables = new ArrayList<>();
        this.tableName = tableName;
    }

    public void init(Identifier identifier, Schema schema) {
        this.tables.add(identifier);
        this.schema = schema;
    }

    public ListMergedJdbcTableInfo merge(Identifier otherTableId, Schema other) {
        if(schema==null){
            init(otherTableId,other);
            return this;
        }

        schema = JdbcSchemaUtils.mergeSchema(location(), schema, otherTableId.getFullName(), other);
        tables.add(otherTableId);
        return this;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    @Override
    public String location() {
        return tables.stream().map(Identifier::toString)
                .collect(Collectors.joining(","));
    }

    @Override
    public List<Identifier> identifiers() {
        return tables;
    }

    @Override
    public String tableName() {
        return tableName;
    }

    @Override
    public String toPaimonTableName() {
        return tableName;
    }

    @Override
    public Schema schema() {
        return schema;
    }
}
