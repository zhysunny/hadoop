package org.apache.hadoop.util;

/**
 * 系统环境，分windows和Linux
 * @author 章云
 * @date 2019/7/30 10:19
 */
public class SystemEnv {

    private static final String systemEnv = System.getProperty("os.name");
    private static final String WINDOWS = "windows";
    private static final String LINUX = "linux";

    /**
     * 判断当前环境是否是windows
     * @return
     */
    public static final boolean isWindows() {
        return systemEnv.startsWith(WINDOWS);
    }

    /**
     * 判断当前环境是否是linux
     * @return
     */
    public static final boolean isLinux() {
        return systemEnv.startsWith(LINUX);
    }

}
