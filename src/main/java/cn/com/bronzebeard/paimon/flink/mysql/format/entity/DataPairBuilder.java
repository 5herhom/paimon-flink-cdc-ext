package cn.com.bronzebeard.paimon.flink.mysql.format.entity;

import java.util.List;

/**
 * @author sherhomhuang
 * @date 2024/09/13 18:19
 * @description
 */
public final class DataPairBuilder {
    private String srcDatabasePattern;
    private String srcTablePattern;
    private String tgtDatabase;
    private String tgtTable;
    private List<String> pkList;
    private List<String> dtColumns;
    private List<String> targetDtColumnNames;

    private DataPairBuilder() {
    }

    public static DataPairBuilder aDataPair() {
        return new DataPairBuilder();
    }

    public DataPairBuilder srcDatabasePattern(String srcDatabasePattern) {
        this.srcDatabasePattern = srcDatabasePattern;
        return this;
    }

    public DataPairBuilder srcTablePattern(String srcTablePattern) {
        this.srcTablePattern = srcTablePattern;
        return this;
    }

    public DataPairBuilder tgtDatabase(String tgtDatabase) {
        this.tgtDatabase = tgtDatabase;
        return this;
    }

    public DataPairBuilder tgtTable(String tgtTable) {
        this.tgtTable = tgtTable;
        return this;
    }

    public DataPairBuilder pkList(List<String> pkList) {
        this.pkList = pkList;
        return this;
    }

    public DataPairBuilder dtColumns(List<String> dtColumns) {
        this.dtColumns = dtColumns;
        return this;
    }

    public DataPairBuilder targetDtColumnNames(List<String> targetDtColumnNames) {
        this.targetDtColumnNames = targetDtColumnNames;
        return this;
    }

    public DataPair build() {
        DataPair dataPair = new DataPair();
        dataPair.setSrcDatabasePattern(srcDatabasePattern);
        dataPair.setSrcTablePattern(srcTablePattern);
        dataPair.setTgtDatabase(tgtDatabase);
        dataPair.setTgtTable(tgtTable);
        dataPair.setPkList(pkList);
        dataPair.setDtColumns(dtColumns);
        dataPair.setTargetDtColumnNames(targetDtColumnNames);
        return dataPair;
    }
}
