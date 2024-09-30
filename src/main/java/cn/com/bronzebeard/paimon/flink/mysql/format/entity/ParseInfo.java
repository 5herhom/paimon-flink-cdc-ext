package cn.com.bronzebeard.paimon.flink.mysql.format.entity;

import org.apache.paimon.flink.action.cdc.CdcMetadataConverter;
import org.apache.paimon.flink.action.cdc.ComputedColumn;

import java.io.Serializable;
import java.util.List;

/**
 * @author sherhomhuang
 * @date 2024/09/13 23:55
 * @description
 */
public class ParseInfo implements Serializable {
    private String tgtDb;
    private String tgtTable;
    private Boolean dt;
    private List<ComputedColumn> computedColumns;
    private CdcMetadataConverter[] metadataConverters;

    public String getTgtDb() {
        return tgtDb;
    }

    public void setTgtDb(String tgtDb) {
        this.tgtDb = tgtDb;
    }

    public String getTgtTable() {
        return tgtTable;
    }

    public void setTgtTable(String tgtTable) {
        this.tgtTable = tgtTable;
    }

    public List<ComputedColumn> getComputedColumns() {
        return computedColumns;
    }

    public void setComputedColumns(List<ComputedColumn> computedColumns) {
        this.computedColumns = computedColumns;
    }

    public CdcMetadataConverter[] getMetadataConverters() {
        return metadataConverters;
    }

    public void setMetadataConverters(CdcMetadataConverter[] metadataConverters) {
        this.metadataConverters = metadataConverters;
    }

    public Boolean getDt() {
        return dt;
    }

    public void setDt(Boolean dt) {
        this.dt = dt;
    }
}
