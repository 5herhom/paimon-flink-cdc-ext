package cn.com.bronzebeard.paimon.flink.mysql.format.entity;

import cn.com.bronzebeard.paimon.flink.common.constants.MetaColumnType;
import cn.com.bronzebeard.paimon.flink.mysql.format.constants.SourceType;
import org.apache.flink.configuration.Configuration;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author sherhomhuang
 * @date 2024/05/10 16:08
 * @description
 */
public class PipelineInfo {
    String ip;
    String port;
    String username;
    String pwd;

    List<DataPair> dataPairList;
    Map<String, String> sourceConfig;
    Integer sinkTypeCode;
    Integer sourceTypeCode;
    Map<String, String> sinkConfig = new HashMap<>();
    Map<String, String> catalogConfig = new HashMap<>();
    Map<String, String> globalConfig = new HashMap<>();

    private Map<MetaColumnType, String> metaColumnTypeToTargetColName = Collections.emptyMap();
    public String getIp() {
        return ip;
    }

    public void setIp(String ip) {
        this.ip = ip;
    }

    public String getPort() {
        return port;
    }

    public void setPort(String port) {
        this.port = port;
    }

    public String getUsername() {
        return username;
    }

    public void setUsername(String username) {
        this.username = username;
    }

    public String getPwd() {
        return pwd;
    }

    public void setPwd(String pwd) {
        this.pwd = pwd;
    }

    public List<DataPair> getDataPairList() {
        return dataPairList;
    }

    public void setDataPairList(List<DataPair> dataPairList) {
        this.dataPairList = dataPairList;
    }

    public Map<String, String> getSourceConfig() {
        return sourceConfig;
    }

    public void setSourceConfig(Map<String, String> sourceConfig) {
        this.sourceConfig = sourceConfig;
    }

    public Map<String, String> getSinkConfig() {
        return sinkConfig;
    }

    public void setSinkConfig(Map<String, String> sinkConfig) {
        this.sinkConfig = sinkConfig;
    }

    public Integer getSinkTypeCode() {
        return sinkTypeCode;
    }

    public void setSinkTypeCode(Integer sinkTypeCode) {
        this.sinkTypeCode = sinkTypeCode;
    }


    public Map<String, String> getCatalogConfig() {
        return catalogConfig;
    }

    public void setCatalogConfig(Map<String, String> catalogConfig) {
        this.catalogConfig = catalogConfig;
    }

    public SourceType getSourceType() {
        return SourceType.byCode(getSourceTypeCode());
    }

    public Integer getSourceTypeCode() {
        return sourceTypeCode;
    }

    public void setSourceTypeCode(Integer sourceTypeCode) {
        this.sourceTypeCode = sourceTypeCode;
    }

    public Map<String, String> getGlobalConfig() {
        return globalConfig;
    }

    public void setGlobalConfig(Map<String, String> globalConfig) {
        this.globalConfig = globalConfig;
    }

    public Configuration newSourceConfig() {
        return Configuration.fromMap(sourceConfig);
    }

    public Configuration newSinkConfig() {
        return Configuration.fromMap(sinkConfig);
    }

    public Configuration newGlobalConfig() {
        return Configuration.fromMap(globalConfig);
    }

    public Map<MetaColumnType, String> getMetaColumnTypeToTargetColName() {
        return metaColumnTypeToTargetColName;
    }

    public void setMetaColumnTypeToTargetColName(Map<MetaColumnType, String> metaColumnTypeToTargetColName) {
        this.metaColumnTypeToTargetColName = metaColumnTypeToTargetColName;
    }
}
