package com.github.andy.dlog;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @Author yan.s.g
 * @Date 2016年08月24日 16:51
 */
public class DLoggerFactory {

    private static final ConcurrentHashMap<String, DLogger> loggerStore = new ConcurrentHashMap<String, DLogger>();

    private DLoggerFactory() {
        throw new UnsupportedOperationException();
    }

    public static final DLogger getLogger(String loggerName) {

        DLogger dLogger = loggerStore.get(loggerName);
        if (dLogger == null) {
            synchronized (loggerName.intern()) {
                dLogger = loggerStore.get(loggerName);
                if (dLogger == null) {
                    String dLogFinagleName = DLogConfig.getDLogFinagleName();
                    if (dLogFinagleName != null) {
                        dLogger = new DLogger(loggerName, dLogFinagleName);
                        loggerStore.put(loggerName, dLogger);
                    }
                }
            }
        }

        return dLogger;
    }

    public static final void clearDLoggers() {
        for (Map.Entry<String, DLogger> entry : loggerStore.entrySet()) {
            DLogger dLogger = entry.getValue();
            if (dLogger != null) {
                dLogger.close();
            }
        }
    }
}
