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

package org.apache.hadoop.dfs;

import java.io.*;
import java.net.*;
import java.util.*;

import org.apache.hadoop.io.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.conf.Configuration;

/**
 * 为DFS系统实现抽象文件系统。<br/>
 * 这个对象是最终用户代码与Hadoop DistributedFileSystem交互的方式。
 * @author 章云
 * @date 2019/8/9 14:42
 */
public class DistributedFileSystem extends FileSystem {
    private File workingDir = new File("/user", System.getProperty("user.name")).getAbsoluteFile();

    private String name;

    DFSClient dfs;

    /**
     * 在 namenode 为文件系统构造一个客户机。
     */
    public DistributedFileSystem(InetSocketAddress namenode, Configuration conf) {
        super(conf);
        this.dfs = new DFSClient(namenode, conf);
        this.name = namenode.getHostName() + ":" + namenode.getPort();
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public File getWorkingDirectory() {
        return workingDir;
    }

    private File makeAbsolute(File f) {
        if (isAbsolute(f)) {
            // hdfs一般使用绝对路径
            return f;
        } else {
            // 如果使用相对路径，默认在用户工作目录下
            return new File(workingDir, f.getPath());
        }
    }

    @Override
    public void setWorkingDirectory(File dir) {
        workingDir = makeAbsolute(dir);
    }

    private UTF8 getPath(File file) {
        String path = getDFSPath(makeAbsolute(file));
        return new UTF8(path);
    }

    @Override
    public String[][] getFileCacheHints(File f, long start, long len) throws IOException {
        return dfs.getHints(getPath(f), start, len);
    }

    @Override
    public FSInputStream openRaw(File f) throws IOException {
        return dfs.open(getPath(f));
    }

    @Override
    public FSOutputStream createRaw(File f, boolean overwrite)
            throws IOException {
        return dfs.create(getPath(f), overwrite);
    }

    /**
     * 重命名文件或目录
     */
    @Override
    public boolean renameRaw(File src, File dst) throws IOException {
        return dfs.rename(getPath(src), getPath(dst));
    }

    /**
     * 删除文件f，无论是真正的文件还是dir。
     */
    @Override
    public boolean deleteRaw(File f) throws IOException {
        return dfs.delete(getPath(f));
    }

    @Override
    public boolean exists(File f) throws IOException {
        return dfs.exists(getPath(f));
    }

    @Override
    public boolean isDirectory(File f) throws IOException {
        if (f instanceof DFSFile) {
            return f.isDirectory();
        }
        return dfs.isDirectory(getPath(f));
    }

    @Override
    public boolean isAbsolute(File f) {
        return f.isAbsolute() ||
                f.getPath().startsWith("/") ||
                f.getPath().startsWith("\\");
    }

    @Override
    public long getLength(File f) throws IOException {
        if (f instanceof DFSFile) {
            return f.length();
        }
        DFSFileInfo[] info = dfs.listFiles(getPath(f));
        return info[0].getLen();
    }

    @Override
    public File[] listFilesRaw(File f) throws IOException {
        DFSFileInfo[] info = dfs.listFiles(getPath(f));
        if (info == null) {
            return new File[0];
        } else {
            File[] results = new DFSFile[info.length];
            for (int i = 0; i < info.length; i++) {
                results[i] = new DFSFile(info[i]);
            }
            return results;
        }
    }

    @Override
    public void mkdirs(File f) throws IOException {
        dfs.mkdirs(getPath(f));
    }

    @Override
    public void lock(File f, boolean shared) throws IOException {
        dfs.lock(getPath(f), !shared);
    }

    @Override
    public void release(File f) throws IOException {
        dfs.release(getPath(f));
    }

    @Override
    public void moveFromLocalFile(File src, File dst) throws IOException {
        doFromLocalFile(src, dst, true);
    }

    @Override
    public void copyFromLocalFile(File src, File dst) throws IOException {
        doFromLocalFile(src, dst, false);
    }

    private void doFromLocalFile(File src, File dst, boolean deleteSource) throws IOException {
        if (exists(dst)) {
            if (!isDirectory(dst)) {
                throw new IOException("Target " + dst + " already exists");
            } else {
                dst = new File(dst, src.getName());
                if (exists(dst)) {
                    throw new IOException("Target " + dst + " already exists");
                }
            }
        }
        FileSystem localFs = getNamed("local", getConf());
        if (localFs.isDirectory(src)) {
            mkdirs(dst);
            File[] contents = localFs.listFiles(src);
            for (int i = 0; i < contents.length; i++) {
                doFromLocalFile(contents[i], new File(dst, contents[i].getName()), deleteSource);
            }
        } else {
            byte[] buf = new byte[FSConstants.BLOCK_SIZE];
            InputStream in = localFs.open(src);
            try {
                OutputStream out = create(dst);
                try {
                    int bytesRead = in.read(buf);
                    while (bytesRead >= 0) {
                        out.write(buf, 0, bytesRead);
                        bytesRead = in.read(buf);
                    }
                } finally {
                    out.close();
                }
            } finally {
                in.close();
            }
        }
        if (deleteSource) {
            localFs.delete(src);
        }
    }

    @Override
    public void copyToLocalFile(File src, File dst) throws IOException {
        if (dst.exists()) {
            if (!dst.isDirectory()) {
                throw new IOException("Target " + dst + " already exists");
            } else {
                dst = new File(dst, src.getName());
                if (dst.exists()) {
                    throw new IOException("Target " + dst + " already exists");
                }
            }
        }
        dst = dst.getCanonicalFile();
        FileSystem localFs = getNamed("local", getConf());
        if (isDirectory(src)) {
            localFs.mkdirs(dst);
            File[] contents = listFiles(src);
            for (int i = 0; i < contents.length; i++) {
                copyToLocalFile(contents[i], new File(dst, contents[i].getName()));
            }
        } else {
            byte[] buf = new byte[FSConstants.BLOCK_SIZE];
            InputStream in = open(src);
            try {
                OutputStream out = localFs.create(dst);
                try {
                    int bytesRead = in.read(buf);
                    while (bytesRead >= 0) {
                        out.write(buf, 0, bytesRead);
                        bytesRead = in.read(buf);
                    }
                } finally {
                    out.close();
                }
            } finally {
                in.close();
            }
        }
    }

    @Override
    public File startLocalOutput(File fsOutputFile, File tmpLocalFile) throws IOException {
        if (exists(fsOutputFile)) {
            copyToLocalFile(fsOutputFile, tmpLocalFile);
        }
        return tmpLocalFile;
    }

    /**
     * 将已完成的本地数据移动到DFS目的地
     */
    @Override
    public void completeLocalOutput(File fsOutputFile, File tmpLocalFile) throws IOException {
        moveFromLocalFile(tmpLocalFile, fsOutputFile);
    }

    /**
     * 获取远程DFS文件，放置在tmpLocalFile
     */
    @Override
    public File startLocalInput(File fsInputFile, File tmpLocalFile) throws IOException {
        copyToLocalFile(fsInputFile, tmpLocalFile);
        return tmpLocalFile;
    }

    /**
     * 我们已经完成了本地的东西，所以删除它
     */
    @Override
    public void completeLocalInput(File localFile) throws IOException {
        // 删除本地副本——我们不再需要它了。
        FileUtil.fullyDelete(localFile, getConf());
    }

    @Override
    public void close() throws IOException {
        dfs.close();
    }

    @Override
    public String toString() {
        return "DFS[" + dfs + "]";
    }

    DFSClient getClient() {
        return dfs;
    }

    private String getDFSPath(File f) {
        List l = new ArrayList();
        l.add(f.getName());
        File parent = f.getParentFile();
        while (parent != null) {
            l.add(parent.getName());
            parent = parent.getParentFile();
        }
        StringBuffer path = new StringBuffer();
        path.append(l.get(l.size() - 1));
        for (int i = l.size() - 2; i >= 0; i--) {
            path.append(DFSFile.DFS_FILE_SEPARATOR);
            path.append(l.get(i));
        }
        if (isAbsolute(f) && path.length() == 0) {
            path.append(DFSFile.DFS_FILE_SEPARATOR);
        }
        return path.toString();
    }

    @Override
    public void reportChecksumFailure(File f, FSInputStream in, long start, long length, int crc) {
        //暂时忽略，导致任务失败，并希望当任务是
        //重试后，它会得到一个没有损坏的块的不同副本。
        // FIXME:我们应该把涉及的坏块移到坏块上
        //目录上的datanode，然后重新复制这些块，就这样
        //没有数据丢失。任务可能会失败，但重试之后应该会成功。
    }

    @Override
    public long getBlockSize() {
        return FSConstants.BLOCK_SIZE;
    }

    /**
     * 返回文件系统的总原始容量，不考虑复制。
     */
    public long getRawCapacity() throws IOException {
        return dfs.totalRawCapacity();
    }

    /**
     * 返回文件系统中总的原始使用空间，不考虑复制。
     */
    public long getRawUsed() throws IOException {
        return dfs.totalRawUsed();
    }

    /**
     * 返回文件系统中所有文件的总大小。
     */
    public long getUsed() throws IOException {
        long used = 0;
        DFSFileInfo[] dfsFiles = dfs.listFiles(getPath(new File("/")));
        for (int i = 0; i < dfsFiles.length; i++) {
            used += dfsFiles[i].getContentsLen();
        }
        return used;
    }

    /**
     * 返回每个datanode的统计信息。
     */
    public DataNodeReport[] getDataNodeStats() throws IOException {
        DatanodeInfo[] dnReport = dfs.datanodeReport();
        DataNodeReport[] reports = new DataNodeReport[dnReport.length];
        for (int i = 0; i < dnReport.length; i++) {
            reports[i] = new DataNodeReport();
            reports[i].name = dnReport[i].getName().toString();
            reports[i].host = dnReport[i].getHost().toString();
            reports[i].capacity = dnReport[i].getCapacity();
            reports[i].remaining = dnReport[i].getRemaining();
            reports[i].lastUpdate = dnReport[i].lastUpdate();
        }
        return reports;
    }
}
