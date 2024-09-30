package cn.com.bronzebeard.paimon.flink.common.util;

import cn.com.bronzebeard.paimon.flink.common.exception.GlobalException;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import java.util.Collection;

/**
 * @author Sherhom
 * @date 2020/9/2 20:12
 */
public class Asset {
    public static void isNull(Object o, String message) {
        if (o != null)
            throwError(message);
    }

    public static void notNull(Object o, String message) {
        if (o == null)
            throwError(message);
    }

    public static void notNull(Object o, String pattern, Object... args) {
        if (o == null)
            throwError(pattern, args);
    }

    public static void isPositive(Object o, String message) {
        if (o == null || Integer.valueOf(o.toString()) <= 0)
            throwError(message);
    }

    public static void isTrue(boolean flag, String message) {
        if (!flag)
            throwError(message);
    }

    public static void isTrue(boolean flag, String pattern, Object... args) {
        if (!flag)
            throwError(pattern, args);
    }

    public static void isFalse(boolean flag, String pattern, Object... args) {
        isTrue(!flag, pattern, args);
    }

    public static void isFalse(boolean flag, String message) {
        isTrue(!flag, message);
    }

    public static void throwError(String message) {
        throw new GlobalException(message);
    }

    public static void throwError(String pattern, Object... args) {
        throw new GlobalException(pattern, args);
    }

    public static void isNotBlank(String str, String message) {
        if (StringUtils.isBlank(str))
            throwError(message);
    }

    public static void isNotBlank(String str, String message, Object... args) {
        if (StringUtils.isBlank(str))
            throwError(message, args);
    }

    public static void isEmpty(Collection c, String pattern, Object... args) {
        isTrue(CollectionUtils.isEmpty(c), pattern, args);
    }

    public static void isEmpty(Collection c, String message) {
        isTrue(CollectionUtils.isEmpty(c), message);
    }


    public static void isNotEmpty(Collection c, String pattern, Object... args) {
        isFalse(CollectionUtils.isEmpty(c), pattern, args);
    }

    public static void isNotEmpty(Collection c, String message) {
        isFalse(CollectionUtils.isEmpty(c), message);
    }

    public static class ThrowableFactory {
        private static ThreadLocal<Class<? extends Throwable>> localErrorType = new ThreadLocal<Class<? extends Throwable>>() {
            @Override
            protected Class<? extends Throwable> initialValue() {
                return GlobalException.class;
            }
        };

    }
}
