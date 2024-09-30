package cn.com.bronzebeard.paimon.flink.utils;

import org.apache.paimon.data.Timestamp;
import org.apache.paimon.types.TimestampType;
import org.apache.paimon.utils.TypeUtils;
import org.junit.jupiter.api.Test;

/**
 * @author sherhomhuang
 * @date 2024/09/19 16:06
 * @description
 */
public class TypeUtilTest {
    @Test
    public void test01() {
        String value = "2022-12-29 11:07:49.000000";
        Timestamp t = (Timestamp)TypeUtils.castFromCdcValueString(value, new TimestampType());
        System.out.println(t);
    }
}
