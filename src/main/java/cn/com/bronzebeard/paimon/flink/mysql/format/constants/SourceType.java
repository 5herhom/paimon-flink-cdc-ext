package cn.com.bronzebeard.paimon.flink.mysql.format.constants;

import cn.com.bronzebeard.paimon.flink.common.exception.GlobalException;
import org.apache.commons.lang3.StringUtils;
import org.apache.paimon.flink.action.cdc.SyncJobHandler;

/**
 * @author sherhomhuang
 * @date 2024/05/10 16:25
 * @description
 */
public enum SourceType {
    MYSQL("mysql", 0);
    private String comment;
    private int code;

    SourceType(String comment, int code) {
        this.comment = comment;
        this.code = code;
    }

    public static SourceType byCode(Object obj) {
        if (obj == null)
            return null;
        Integer code;
        try {
            code = Integer.parseInt(obj.toString());
        } catch (Throwable e) {
            return null;
        }
        SourceType[] types = values();
        for (SourceType SinkType : types) {
            if (SinkType.code == code) {
                return SinkType;
            }
        }
        return null;
    }

    public static boolean validateComment(String comment) {
        if (StringUtils.isBlank(comment))
            return false;
        SourceType[] types = values();
        for (SourceType SinkType : types) {
            if (SinkType.comment.equalsIgnoreCase(comment)) {
                return true;
            }
        }
        return false;
    }

    public static SourceType commentOf(String comment) {
        if (StringUtils.isBlank(comment))
            return null;
        SourceType[] types = values();
        for (SourceType SinkType : types) {
            if (SinkType.comment.equalsIgnoreCase(comment)) {
                return SinkType;
            }
        }
        return null;
    }

    public String getComment() {
        return comment;
    }


    public int getCode() {
        return code;
    }

    public SyncJobHandler.SourceType toPaimonCdcSourceType() {
        if (this.equals(MYSQL)) {
            return SyncJobHandler.SourceType.MYSQL;
        } else {
            throw  new GlobalException("Not support type: %s", this);
        }
    }
}
