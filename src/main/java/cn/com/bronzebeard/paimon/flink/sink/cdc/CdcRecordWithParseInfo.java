package cn.com.bronzebeard.paimon.flink.sink.cdc;

import cn.com.bronzebeard.paimon.flink.mysql.format.entity.ParseInfo;
import org.apache.paimon.flink.sink.cdc.CdcRecord;
import org.apache.paimon.types.RowKind;

import java.util.Map;

/**
 * @author sherhomhuang
 * @date 2024/09/14 10:49
 * @description
 */
public class CdcRecordWithParseInfo extends CdcRecord {
    ParseInfo parseInfo;

    public CdcRecordWithParseInfo(ParseInfo parseInfo, RowKind kind, Map<String, String> fields) {
        super(kind, fields);
        this.parseInfo = parseInfo;
    }

    public ParseInfo getParseInfo() {
        return parseInfo;
    }

    public void setParseInfo(ParseInfo parseInfo) {
        this.parseInfo = parseInfo;
    }

    public CdcRecord rawRecord() {
        return new CdcRecord(kind(), fields());
    }
}
