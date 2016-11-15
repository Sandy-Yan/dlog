package com.github.andy.dlog;

import  com.github.andy.common.properties.PropertiesManager;
/**
 * @Author yan.s.g
 * @Date 2016年08月24日 18:31
 */
public class DLogConfig {

    private static final String CONTEXT_PROPERTIES_FILE = "context.properties";

    private DLogConfig() {
        throw new UnsupportedOperationException();
    }

    public static String getDLogFinagleName() {
        return PropertiesManager.getString(CONTEXT_PROPERTIES_FILE, "dLogFinagleName", null);
    }
}
