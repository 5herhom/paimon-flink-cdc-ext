package cn.com.bronzebeard.paimon.flink;

import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author sherhomhuang
 * @date 2024/05/09 17:04
 * @description
 */
public class LogTest {
    private static final Logger LOG = LoggerFactory.getLogger(LogTest.class);
    @Test
    public void test(){
        LOG.info("asdasd");
        LOG.debug("debug:asdasd");
    }
}
