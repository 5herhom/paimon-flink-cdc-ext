package cn.com.bronzebeard.paimon.flink.common.expression;

import cn.com.bronzebeard.paimon.flink.common.util.DateUtil;
import org.apache.paimon.flink.action.cdc.Expression;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DataTypes;

import java.text.ParseException;

/**
 * @author sherhomhuang
 * @date 2024/09/13 22:27
 * @description
 */
public class AutoDateFormatExpression implements Expression {
    private final String fieldReference;
    private final String pattern;
    private final String defaultValue;

    public AutoDateFormatExpression(String fieldReference, String pattern1) {
        this(fieldReference, pattern1, null);
    }

    public AutoDateFormatExpression(String fieldReference, String pattern, String defaultValue) {
        this.fieldReference = fieldReference;
        this.pattern = pattern;
        this.defaultValue = defaultValue;
    }

    @Override
    public String fieldReference() {
        return fieldReference;
    }

    @Override
    public DataType outputType() {
        return DataTypes.STRING();
    }

    @Override
    public String eval(String input) {
        try {
            String res = DateUtil.autoFormatDateTime(input, pattern);
            return res == null ? defaultValue : res;
        } catch (Exception e) {
            return defaultValue;
        }
    }
}
