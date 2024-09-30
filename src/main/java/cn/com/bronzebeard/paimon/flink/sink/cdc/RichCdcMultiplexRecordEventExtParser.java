package cn.com.bronzebeard.paimon.flink.sink.cdc;

import cn.com.bronzebeard.paimon.flink.common.exception.GlobalException;
import cn.com.bronzebeard.paimon.flink.common.util.Asset;
import cn.com.bronzebeard.paimon.flink.mysql.format.PatternMatchedToTargetMapper;
import cn.com.bronzebeard.paimon.flink.mysql.format.entity.DataPair;
import cn.com.bronzebeard.paimon.flink.mysql.format.entity.ParseInfo;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.flink.action.cdc.ComputedColumn;
import org.apache.paimon.flink.action.cdc.TableNameConverter;
import org.apache.paimon.flink.sink.cdc.*;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.types.DataField;
import org.apache.paimon.utils.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.util.*;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static org.apache.paimon.utils.Preconditions.checkNotNull;

/**
 * @author sherhomhuang
 * @date 2024/05/13 11:38
 * @description
 */
public class RichCdcMultiplexRecordEventExtParser implements EventParser<RichCdcMultiplexRecord> {

    private static final Logger LOG =
            LoggerFactory.getLogger(RichCdcMultiplexRecordEventExtParser.class);

    @Nullable
    private final NewTableSchemaBuilder schemaBuilder;
    @Nullable
    private final Pattern includingPattern;
    @Nullable
    private final Pattern excludingPattern;
    private final TableNameConverter tableNameConverter;
    private final Map<String, RichEventParser> parsers = new HashMap<>();
    private final Set<String> includedTables = new HashSet<>();
    private final Set<String> excludedTables = new HashSet<>();
    private final Set<String> createdTables = new HashSet<>();

    private RichCdcMultiplexRecord record;
    private String currentTable;
    private boolean shouldSynchronizeCurrentTable;
    private RichEventParser currentParser;

    private final PatternMatchedToTargetMapper<ParseInfo> mysqlParseInfoMapper;


    public RichCdcMultiplexRecordEventExtParser(
            @Nullable NewTableSchemaBuilder schemaBuilder,
            @Nullable Pattern includingPattern,
            @Nullable Pattern excludingPattern,
            TableNameConverter tableNameConverter, PatternMatchedToTargetMapper<ParseInfo> mysqlParseInfoMapper) {
        this.schemaBuilder = schemaBuilder;
        this.includingPattern = includingPattern;
        this.excludingPattern = excludingPattern;
        this.tableNameConverter = tableNameConverter;
        this.mysqlParseInfoMapper = mysqlParseInfoMapper;
    }

    @Override
    public void setRawEvent(RichCdcMultiplexRecord record) {
        this.record = record;
        this.currentTable = record.tableName();
        this.shouldSynchronizeCurrentTable = shouldSynchronizeCurrentTable();
        if (shouldSynchronizeCurrentTable) {
            this.currentParser = parsers.computeIfAbsent(currentTable, t -> new RichEventParser());
            this.currentParser.setRawEvent(record.toRichCdcRecord());
        }
    }

    @Override
    public String parseTableName() {
        // database synchronization needs this, so we validate the record here
        if (record.databaseName() == null || record.tableName() == null) {
            throw new IllegalArgumentException(
                    "Cannot synchronize record when database name or table name is unknown. "
                            + "Invalid record is:\n"
                            + record);
        }
        return tableNameConverter.convert(Identifier.create(record.databaseName(), currentTable));
    }

    @Override
    public List<DataField> parseSchemaChange() {
        return shouldSynchronizeCurrentTable
                ? currentParser.parseSchemaChange()
                : Collections.emptyList();
    }

    @Override
    public List<CdcRecord> parseRecords() {
        if (!shouldSynchronizeCurrentTable) {
            return Collections.emptyList();
        }
        List<CdcRecord> cdcRecords = currentParser.parseRecords();
        String fullTableName = String.format(DataPair.FULL_TABLE_NAME_PATTERN, record.databaseName(), record.tableName());
        List<ParseInfo> parseInfoList = mysqlParseInfoMapper.getTargetByMatched(fullTableName);
        if (parseInfoList.isEmpty()) {
            Asset.isTrue(mysqlParseInfoMapper.addNewMatchedToMapper(fullTableName), "Table %s can not add to mapper, please check config.", fullTableName);
            parseInfoList = mysqlParseInfoMapper.getTargetByMatched(fullTableName);
        }
        List<ParseInfo> finalParseInfoList = parseInfoList;
        List<CdcRecord> results = cdcRecords.stream().flatMap(rec -> {
            Map<String, String> fields = rec.fields();
            return finalParseInfoList.stream().map(parseInfo -> {
                Map<String, String> resultMap = finalParseInfoList.size() == 1 ? fields : new HashMap<>(fields);
                List<ComputedColumn> computedColumns = parseInfo.getComputedColumns();
                for (ComputedColumn computedColumn : computedColumns) {
                    resultMap.put(
                            computedColumn.columnName(),
                            computedColumn.eval(resultMap.get(computedColumn.fieldReference())));
                }
                return new CdcRecordWithParseInfo(parseInfo, rec.kind(), resultMap);
            });
        }).collect(Collectors.toList());
        return results;
    }

    @Override
    public Optional<Schema> parseNewTable() {
        if (shouldCreateCurrentTable()) {
            checkNotNull(schemaBuilder, "NewTableSchemaBuilder hasn't been set.");
            return schemaBuilder.build(record);
        }

        return Optional.empty();
    }

    private boolean shouldSynchronizeCurrentTable() {
        // In case the record is incomplete, we let the null value pass validation
        // and handle the null value when we really need it
        if (currentTable == null) {
            return true;
        }

        if (includedTables.contains(currentTable)) {
            return true;
        }
        if (excludedTables.contains(currentTable)) {
            return false;
        }

        boolean shouldSynchronize = true;
        if (includingPattern != null) {
            shouldSynchronize = includingPattern.matcher(currentTable).matches();
        }
        if (excludingPattern != null) {
            shouldSynchronize =
                    shouldSynchronize && !excludingPattern.matcher(currentTable).matches();
        }
        if (!shouldSynchronize) {
            LOG.debug(
                    "Source table {} won't be synchronized because it was excluded. ",
                    currentTable);
            excludedTables.add(currentTable);
            return false;
        }

        includedTables.add(currentTable);
        return true;
    }

    private boolean shouldCreateCurrentTable() {
        return shouldSynchronizeCurrentTable
                && !record.fields().isEmpty()
                && createdTables.add(parseTableName());
    }
}
