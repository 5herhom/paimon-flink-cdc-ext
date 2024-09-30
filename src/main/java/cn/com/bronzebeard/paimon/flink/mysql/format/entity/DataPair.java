package cn.com.bronzebeard.paimon.flink.mysql.format.entity;

import org.apache.commons.collections.CollectionUtils;

import java.util.Collections;
import java.util.List;

/**
 * @author sherhomhuang
 * @date 2024/05/10 16:13
 * @description
 */
public class DataPair {
    public static final String DEFAULT_DT_COL_NAME = "dt";
    public static final List<String> DEFAULT_TARGET_DT_LIST = Collections.singletonList(DEFAULT_DT_COL_NAME);
    public static final String FULL_REG_TABLE_NAME_PATTERN = "%s\\.%s";
    public static final String FULL_TABLE_NAME_PATTERN = "%s.%s";
    private String srcDatabasePattern;
    private String srcTablePattern;
    private String tgtDatabase;
    private String tgtTable;
    private List<String> pkList;
    private List<String> dtColumns;
    private List<String> targetDtColumnNames = Collections.emptyList();

    public String getSrcDatabasePattern() {
        return srcDatabasePattern;
    }

    public void setSrcDatabasePattern(String srcDatabasePattern) {
        this.srcDatabasePattern = srcDatabasePattern;
    }

    public String getSrcTablePattern() {
        return srcTablePattern;
    }

    public void setSrcTablePattern(String srcTablePattern) {
        this.srcTablePattern = srcTablePattern;
    }

    public String getTgtDatabase() {
        return tgtDatabase;
    }

    public void setTgtDatabase(String tgtDatabase) {
        this.tgtDatabase = tgtDatabase;
    }

    public String getTgtTable() {
        return tgtTable;
    }

    public void setTgtTable(String tgtTable) {
        this.tgtTable = tgtTable;
    }

    public List<String> getPkList() {
        return pkList;
    }

    public void setPkList(List<String> pkList) {
        this.pkList = pkList;
    }

    public boolean isDt() {
        return CollectionUtils.isNotEmpty(getDtColumns());
    }

    public List<String> getDtColumns() {
        return dtColumns;
    }

    public void setDtColumns(List<String> dtColumns) {
        this.dtColumns = dtColumns;
    }

    public String getFullSrcTableNamePattern() {
        return String.format(FULL_REG_TABLE_NAME_PATTERN, srcDatabasePattern, srcTablePattern);
    }

    public String getFullTgtTableName() {
        return String.format(FULL_TABLE_NAME_PATTERN, tgtDatabase, tgtTable);
    }

    public List<String> getTargetDtColumnNames() {
        if (CollectionUtils.isEmpty(targetDtColumnNames) && isDt()) {
            targetDtColumnNames = DEFAULT_TARGET_DT_LIST;
        }
        return targetDtColumnNames;
    }

    public void setTargetDtColumnNames(List<String> targetDtColumnNames) {
        this.targetDtColumnNames = targetDtColumnNames;
    }
}
