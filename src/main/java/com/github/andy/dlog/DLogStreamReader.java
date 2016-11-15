package com.github.andy.dlog;

import com.twitter.distributedlog.AsyncLogReader;
import com.twitter.distributedlog.DistributedLogConfiguration;
import com.twitter.distributedlog.DistributedLogManager;
import com.twitter.distributedlog.LogRecordWithDLSN;
import com.twitter.distributedlog.namespace.DistributedLogNamespace;
import com.twitter.distributedlog.namespace.DistributedLogNamespaceBuilder;
import com.twitter.distributedlog.util.FutureUtils;
import com.twitter.util.CountDownLatch;
import com.twitter.util.Duration;
import com.twitter.util.FutureEventListener;
import org.joda.time.DateTime;

import java.net.URI;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.google.common.base.Charsets.UTF_8;

/**
 * @Author yan.s.g
 * @Date 2016年08月24日 14:51
 */
public class DLogStreamReader {

    private final static String HELP = "DLogStreamReader <uri> <string> <seconds>";

    public static void main(String[] args) throws Exception {

        if (3 != args.length) {
            System.out.println(HELP);
            return;
        }

        // distributedlog://192.168.1.205:2181/messaging/distributedlog/mynamespace
        String dlUriStr = args[0];
        // log stream name
        final String streamName = args[1];

        final int rewindSeconds = Integer.parseInt(args[2]);

        URI uri = URI.create(dlUriStr);
        DistributedLogConfiguration conf = new DistributedLogConfiguration();
        DistributedLogNamespace namespace = DistributedLogNamespaceBuilder.newBuilder()//
                .conf(conf)//
                .uri(uri)//
                .build();

        // open the dlog stream
        DistributedLogManager dlm = namespace.openLog(streamName);

        try {
            readLoop(dlm, rewindSeconds);
        } finally {
            dlm.close();
            namespace.close();
        }
    }

    private static void readLoop(final DistributedLogManager dlm, final int rewindSeconds) throws Exception {

        final CountDownLatch keepAliveLatch = new CountDownLatch(1);
        long rewindToTxId = System.currentTimeMillis() - TimeUnit.MILLISECONDS.convert(rewindSeconds, TimeUnit.SECONDS);

        final AsyncLogReader reader = FutureUtils.result(dlm.openAsyncLogReader(rewindToTxId));
        final AtomicBoolean caughtup = new AtomicBoolean(false);
        final FutureEventListener<LogRecordWithDLSN> readListener = new FutureEventListener<LogRecordWithDLSN>() {
            @Override
            public void onFailure(Throwable cause) {
                cause.printStackTrace(System.err);
                keepAliveLatch.countDown();
            }

            @Override
            public void onSuccess(LogRecordWithDLSN record) {
                long createTime = record.getTransactionId();
                long diffInMilliseconds = System.currentTimeMillis() - createTime;
                if (!caughtup.get() && diffInMilliseconds < 2000) {
                    caughtup.set(true);
                }

                reader.readNext().addEventListener(this);
            }
        };
        reader.readNext().addEventListener(readListener);

        keepAliveLatch.await();
        FutureUtils.result(reader.asyncClose(), Duration.apply(5, TimeUnit.SECONDS));
    }

}
