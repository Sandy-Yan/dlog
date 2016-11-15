package com.github.andy.dlog;

import com.twitter.distributedlog.DLSN;
import com.twitter.distributedlog.service.DistributedLogClient;
import com.twitter.distributedlog.service.DistributedLogClientBuilder;
import com.twitter.finagle.thrift.ClientId;
import com.twitter.util.FutureEventListener;
import com.github.andy.common.util.format.StringFormatUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;

import static com.google.common.base.Charsets.UTF_8;

/**
 * @Author yan.s.g
 * @Date 2016年08月24日 16:42
 */
public class DLogger {

    private static Logger logger = LoggerFactory.getLogger(DLogger.class);

    private final DistributedLogClient client;

    private final String loggerName;

    /**
     * @param loggerName
     * @param dLogFinagleName
     */
    public DLogger(String loggerName, String dLogFinagleName) {
        this.loggerName = loggerName;
        this.client = DistributedLogClientBuilder.newBuilder()//
                .clientId(ClientId.apply(loggerName))//
                .name(loggerName)//
                .thriftmux(true)//
                .finagleNameStr(dLogFinagleName)//
                .build();
    }

    public void write(String format, Object... args) {
        ByteBuffer bb = ByteBuffer.wrap(format == null ? "null".getBytes(UTF_8) : StringFormatUtil.format(format, args).getBytes(UTF_8));
        client.write(loggerName, bb).addEventListener(new FutureEventListener<DLSN>() {
            @Override
            public void onFailure(Throwable cause) {
                logger.error("## Encountered error on writing data.", cause);
            }

            @Override
            public void onSuccess(DLSN value) {
                //
            }
        });
    }

    public void close() {
        client.close();
    }
}
