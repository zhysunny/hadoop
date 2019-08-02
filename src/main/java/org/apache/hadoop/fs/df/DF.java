/**
 * Copyright 2005 The Apache Software Foundation
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.fs.df;

import java.io.IOException;
import java.io.InputStreamReader;
import java.io.BufferedReader;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.ConfigConstants;
import org.apache.hadoop.util.SystemEnv;

/**
 * 文件系统磁盘空间使用统计。使用unix 'df'程序。测试在Linux, FreeBSD, Cygwin。
 * @author 章云
 * @date 2019/8/2 10:23
 */
public abstract class DF {
    /**
     * DF查看的目录
     */
    protected String dirPath;
    /**
     * DF刷新间隔(毫秒)
     */
    protected long dfInterval;
    /**
     * 上次执行doDF()时
     */
    protected long lastDF;
    /**
     * 文件系统
     */
    protected String filesystem;
    /**
     * 总容量(字节)
     */
    protected long capacity;
    /**
     * 已用容量(字节)
     */
    protected long used;
    /**
     * 可用容量(字节)
     */
    protected long available;
    /**
     * 已用容量占比
     */
    protected int percentUsed;
    /**
     * 挂载点
     */
    protected String mount;

    public DF(String path, Configuration conf) throws IOException {
        this(path, conf.getLong(ConfigConstants.DFS_DF_INTERVAL, ConfigConstants.DFS_DF_INTERVAL_DEFAULT));
    }

    public DF(String path, long dfInterval) throws IOException {
        this.dirPath = path;
        this.dfInterval = dfInterval;
        lastDF = (dfInterval < 0) ? 0 : -dfInterval;
        this.doDF();
    }

    /**
     * 执行DF命令
     * @throws IOException
     */
    protected abstract void doDF() throws IOException;

    /**
     * df命令
     * @return
     */
    protected abstract Object getExecString();

    /**
     * 解析命令执行的结果
     * @param lines
     * @throws IOException
     */
    protected abstract void parseExecResult(BufferedReader lines) throws IOException;

    /// ACCESSORS

    public String getDirPath() {
        return dirPath;
    }

    public String getFilesystem() throws IOException {
        doDF();
        return filesystem;
    }

    public long getCapacity() throws IOException {
        doDF();
        return capacity;
    }

    public long getUsed() throws IOException {
        doDF();
        return used;
    }

    public long getAvailable() throws IOException {
        doDF();
        return available;
    }

    public int getPercentUsed() throws IOException {
        doDF();
        return percentUsed;
    }

    public String getMount() throws IOException {
        doDF();
        return mount;
    }

    @Override
    public String toString() {
        return "df -k " + mount + "\n" +
                filesystem + "\t" +
                capacity + "\t" +
                used + "\t" +
                available + "\t" +
                percentUsed + "%\t" +
                mount;
    }

    public static void main(String[] args) throws Exception {
        String path = ".";
        if (args.length > 0) {
            path = args[0];
        }
        System.out.println(DFFactory.getDF(path, ConfigConstants.DFS_DF_INTERVAL_DEFAULT).toString());
    }
}
