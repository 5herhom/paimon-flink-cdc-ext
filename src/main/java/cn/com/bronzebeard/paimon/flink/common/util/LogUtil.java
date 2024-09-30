package cn.com.bronzebeard.paimon.flink.common.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author Sherhom
 * @date 2020/9/4 11:33
 */
public class LogUtil {
    public final static Logger log = LoggerFactory.getLogger(LogUtil.class);

    public static void printStackTrace(Logger log, Throwable e) {
        log.error(ExceptionUtil.toStackTraceString(e));
    }
    public static void printStackTrace(Throwable e) {
        log.error(ExceptionUtil.toStackTraceString(e));
    }
}
