package org.apache.hadoop.fs.df;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.ConfigConstants;
import org.apache.hadoop.util.SystemEnv;

import java.io.IOException;

/**
 * 根据当前环境获得DF类
 * @author 章云
 * @date 2019/8/2 11:06
 */
public class DFFactory {

    public static DF getDF(String path, Configuration conf) throws IOException {
        return getDF(path, conf.getLong(ConfigConstants.DFS_DF_INTERVAL, ConfigConstants.DFS_DF_INTERVAL_DEFAULT));
    }

    public static DF getDF(String path, long dfInterval) throws IOException {
        if (SystemEnv.isWindows()) {
            return new WindowDF(path, dfInterval);
        } else {
            return new LinuxDF(path, dfInterval);
        }
    }

}
