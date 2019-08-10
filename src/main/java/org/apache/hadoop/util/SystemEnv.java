package org.apache.hadoop.util;

import java.util.Locale;

/**
 * 系统环境，分windows和Linux
 * @author 章云
 * @date 2019/7/30 10:19
 */
public class SystemEnv {

    private static final String SYSTEM_ENV = System.getProperty("os.name").toLowerCase(Locale.ENGLISH);
    private static final String WINDOWS = "windows";
    private static final String LINUX = "linux";

    /**
     * 判断当前环境是否是windows
     * @return
     */
    public static final boolean isWindows() {
        return SYSTEM_ENV.startsWith(WINDOWS);
    }

    /**
     * 判断当前环境是否是linux
     * @return
     */
    public static final boolean isLinux() {
        return SYSTEM_ENV.startsWith(LINUX);
    }

}
