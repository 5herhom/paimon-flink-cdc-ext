package cn.com.bronzebeard.paimon.flink.mysql.format.entity.schema;

import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.flink.action.cdc.schema.AllMergedJdbcTableInfo;
import org.apache.paimon.flink.action.cdc.schema.JdbcTableInfo;
import org.apache.paimon.flink.action.cdc.schema.ShardsMergedJdbcTableInfo;
import org.apache.paimon.flink.action.cdc.schema.UnmergedJdbcTableInfo;
import org.apache.paimon.schema.Schema;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * @author sherhomhuang
 * @date 2024/05/11 16:31
 * @description
 */
public class JdbcSchemasInfo {
    private final List<JdbcSchemaInfo> schemasInfo;

    public JdbcSchemasInfo() {
        this.schemasInfo = new ArrayList<>();
    }

    public void addSchema(Identifier identifier, Schema schema) {
        addSchema(identifier, null, schema);
    }

    public void addSchema(Identifier identifier, String schemaName, Schema schema) {
        JdbcSchemaInfo schemaInfo =
                new JdbcSchemaInfo(identifier, schemaName, !schema.primaryKeys().isEmpty(), schema);
        schemasInfo.add(schemaInfo);
    }

    public List<JdbcSchemaInfo> pkTables() {
        return schemasInfo.stream().filter(JdbcSchemaInfo::isPkTable).collect(Collectors.toList());
    }

    public List<Identifier> nonPkTables() {
        return schemasInfo.stream()
                .filter(jdbcSchemaInfo -> !jdbcSchemaInfo.isPkTable())
                .map(JdbcSchemaInfo::identifier)
                .collect(Collectors.toList());
    }

    public List<JdbcSchemaInfo> getSchemasInfo() {
        return schemasInfo;
    }

    // only merge pk tables now
    public JdbcTableInfo mergeAll() {
        boolean initialized = false;
        AllMergedJdbcTableInfo merged = new AllMergedJdbcTableInfo();
        for (JdbcSchemaInfo jdbcSchemaInfo : schemasInfo) {
            if (!jdbcSchemaInfo.isPkTable()) {
                continue;
            }
            Identifier id = jdbcSchemaInfo.identifier();
            Schema schema = jdbcSchemaInfo.schema();
            if (!initialized) {
                merged.init(id, schema);
                initialized = true;
            } else {
                merged.merge(id, schema);
            }
        }
        return merged;
    }

    // only handle pk tables now
    public List<JdbcTableInfo> toMySqlTableInfos(boolean mergeShards) {
        if (mergeShards) {
            return mergeShards();
        } else {
            return schemasInfo.stream()
                    .filter(JdbcSchemaInfo::isPkTable)
                    .map(e -> new UnmergedJdbcTableInfo(e.identifier(), e.schema()))
                    .collect(Collectors.toList());
        }
    }

    // only merge pk tables now

    /** Merge schemas for tables that have the same table name. */
    private List<JdbcTableInfo> mergeShards() {
        Map<String, ShardsMergedJdbcTableInfo> nameSchemaMap = new HashMap<>();
        for (JdbcSchemaInfo jdbcSchemaInfo : schemasInfo) {
            if (!jdbcSchemaInfo.isPkTable()) {
                continue;
            }
            Identifier id = jdbcSchemaInfo.identifier();
            String tableName = id.getObjectName();

            Schema toBeMerged = jdbcSchemaInfo.schema();
            ShardsMergedJdbcTableInfo current = nameSchemaMap.get(tableName);
            if (current == null) {
                current = new ShardsMergedJdbcTableInfo();
                current.init(id, toBeMerged);
                nameSchemaMap.put(tableName, current);
            } else {
                nameSchemaMap.put(tableName, current.merge(id, toBeMerged));
            }
        }
        return new ArrayList<>(nameSchemaMap.values());
    }

    public static class JdbcSchemaInfo {

        private final Identifier identifier;

        private final String schemaName;

        private final boolean isPkTable;

        private final Schema schema;

        public JdbcSchemaInfo(
                Identifier identifier, String schemaName, boolean isPkTable, Schema schema) {
            this.identifier = identifier;
            this.schemaName = schemaName;
            this.isPkTable = isPkTable;
            this.schema = schema;
        }

        public Identifier identifier() {
            return identifier;
        }

        public String schemaName() {
            return schemaName;
        }

        public boolean isPkTable() {
            return isPkTable;
        }

        public Schema schema() {
            return schema;
        }
    }
}
