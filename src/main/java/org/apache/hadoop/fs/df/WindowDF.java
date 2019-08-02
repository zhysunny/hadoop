package org.apache.hadoop.fs.df;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Locale;
import java.util.StringTokenizer;

/**
 * Windows下查看磁盘空间
 * @author 章云
 * @date 2019/8/2 11:00
 */
public class WindowDF extends DF {

    public WindowDF(String path, long dfInterval) throws IOException {
        super(path, dfInterval);
    }

    @Override
    protected void doDF() throws IOException {
        if (lastDF + dfInterval > System.currentTimeMillis()) {
            return;
        }
        Process process = Runtime.getRuntime().exec(getExecString());
        parseExecResult(new BufferedReader(new InputStreamReader(process.getInputStream())));
        process.destroy();
    }

    @Override
    protected String getExecString() {
        mount = new File(dirPath).getAbsolutePath().substring(0, 2).toUpperCase(Locale.ENGLISH);
        return "wmic LogicalDisk where \"Caption='" + mount + "'\" get FreeSpace, Size /value";
    }

    @Override
    protected void parseExecResult(BufferedReader lines) throws IOException {
        // 跳过标题
        String line = null;
        while ((line = lines.readLine()) != null) {
            if (line.length() == 0) {
                continue;
            }
            StringTokenizer tokens = new StringTokenizer(line, "=");
            String key = tokens.nextToken();
            if ("FreeSpace".equals(key)) {
                available = Long.parseLong(tokens.nextToken());
            }
            if ("Size".equals(key)) {
                capacity = Long.parseLong(tokens.nextToken());
            }
        }
        filesystem = mount;
        used = capacity - available;
        percentUsed = (int) ((double) used / (double) capacity * 100.0);
        lastDF = System.currentTimeMillis();
    }

}
