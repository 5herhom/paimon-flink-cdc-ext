package cn.com.bronzebeard.paimon.flink.mysql.format.constants;

import org.apache.commons.lang3.StringUtils;

/**
 * @author sherhomhuang
 * @date 2024/05/10 16:25
 * @description
 */
public enum SinkType {
    PAIMON("paimon", 0);
    private String comment;
    private int code;

    SinkType(String comment, int code) {
        this.comment = comment;
        this.code = code;
    }

    public static SinkType byCode(Object obj) {
        if (obj == null)
            return null;
        Integer code;
        try {
            code = Integer.parseInt(obj.toString());
        } catch (Throwable e) {
            return null;
        }
        SinkType[] types = values();
        for (SinkType SinkType : types) {
            if (SinkType.code == code) {
                return SinkType;
            }
        }
        return null;
    }

    public static boolean validateComment(String comment) {
        if (StringUtils.isBlank(comment))
            return false;
        SinkType[] types = values();
        for (SinkType SinkType : types) {
            if (SinkType.comment.equalsIgnoreCase(comment)) {
                return true;
            }
        }
        return false;
    }

    public static SinkType commentOf(String comment) {
        if (StringUtils.isBlank(comment))
            return null;
        SinkType[] types = values();
        for (SinkType SinkType : types) {
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

}
