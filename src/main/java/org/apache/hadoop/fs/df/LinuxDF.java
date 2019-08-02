package org.apache.hadoop.fs.df;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.StringTokenizer;

/**
 * Linux下查看磁盘空间
 * @author 章云
 * @date 2019/8/2 11:02
 */
public class LinuxDF extends DF {

    public LinuxDF(String path, long dfInterval) throws IOException {
        super(path, dfInterval);
    }

    @Override
    protected void doDF() throws IOException {
        if (lastDF + dfInterval > System.currentTimeMillis()) {
            return;
        }
        Process process;
        process = Runtime.getRuntime().exec(getExecString());
        try {
            if (process.waitFor() != 0) {
                throw new IOException(new BufferedReader(new InputStreamReader(process.getErrorStream())).readLine());
            }
            parseExecResult(new BufferedReader(new InputStreamReader(process.getInputStream())));
        } catch (InterruptedException e) {
            throw new IOException(e.toString());
        } finally {
            process.destroy();
        }
    }

    @Override
    protected String[] getExecString() {
        return new String[]{"df", "-k", dirPath};
    }

    @Override
    protected void parseExecResult(BufferedReader lines) throws IOException {
        // 跳过标题
        lines.readLine();
        StringTokenizer tokens = new StringTokenizer(lines.readLine(), " \t\n\r\f%");
        filesystem = tokens.nextToken();
        // 对于长文件系统名称
        if (!tokens.hasMoreTokens()) {
            tokens = new StringTokenizer(lines.readLine(), " \t\n\r\f%");
        }
        capacity = Long.parseLong(tokens.nextToken()) * 1024;
        used = Long.parseLong(tokens.nextToken()) * 1024;
        available = Long.parseLong(tokens.nextToken()) * 1024;
        percentUsed = Integer.parseInt(tokens.nextToken());
        mount = tokens.nextToken();
        lastDF = System.currentTimeMillis();
    }

}
