package cn.com.bronzebeard.paimon.flink.mysql.format.entity;

import org.apache.paimon.flink.action.cdc.CdcMetadataConverter;
import org.apache.paimon.flink.action.cdc.ComputedColumn;

import java.util.List;

/**
 * @author sherhomhuang
 * @date 2024/09/14 10:58
 * @description
 */
public final class ParseInfoBuilder {
    private String tgtDb;
    private String tgtTable;
    private Boolean dt;
    private List<ComputedColumn> computedColumns;
    private CdcMetadataConverter[] metadataConverters;

    private ParseInfoBuilder() {
    }

    public static ParseInfoBuilder aParseInfo() {
        return new ParseInfoBuilder();
    }

    public ParseInfoBuilder tgtDb(String tgtDb) {
        this.tgtDb = tgtDb;
        return this;
    }

    public ParseInfoBuilder tgtTable(String tgtTable) {
        this.tgtTable = tgtTable;
        return this;
    }

    public ParseInfoBuilder dt(Boolean dt) {
        this.dt = dt;
        return this;
    }

    public ParseInfoBuilder computedColumns(List<ComputedColumn> computedColumns) {
        this.computedColumns = computedColumns;
        return this;
    }

    public ParseInfoBuilder metadataConverters(CdcMetadataConverter[] metadataConverters) {
        this.metadataConverters = metadataConverters;
        return this;
    }

    public ParseInfo build() {
        ParseInfo parseInfo = new ParseInfo();
        parseInfo.setTgtDb(tgtDb);
        parseInfo.setTgtTable(tgtTable);
        parseInfo.setDt(dt);
        parseInfo.setComputedColumns(computedColumns);
        parseInfo.setMetadataConverters(metadataConverters);
        return parseInfo;
    }
}
