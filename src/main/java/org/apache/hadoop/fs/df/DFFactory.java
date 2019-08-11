package org.apache.hadoop.fs.df;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.Constants;
import org.apache.hadoop.util.SystemEnv;

import java.io.IOException;

/**
 * 根据当前环境获得DF类
 * @author 章云
 * @date 2019/8/2 11:06
 */
public class DFFactory {

    public static DF getDF(String path, Configuration conf) throws IOException {
        return getDF(path, Constants.DFS_DF_INTERVAL);
    }

    public static DF getDF(String path, long dfInterval) throws IOException {
        if (SystemEnv.isWindows()) {
            return new WindowDF(path, dfInterval);
        } else {
            return new LinuxDF(path, dfInterval);
        }
    }

}
