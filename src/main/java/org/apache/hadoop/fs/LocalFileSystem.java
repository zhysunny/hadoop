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

package org.apache.hadoop.fs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.exception.FSError;
import org.apache.hadoop.fs.df.DFFactory;
import org.apache.hadoop.util.Constants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.channels.FileLock;
import java.util.Random;
import java.util.TreeMap;

/**
 * 为本机文件系统实现FileSystem API。
 * @author 章云
 * @date 2019/8/2 13:27
 */
public class LocalFileSystem extends FileSystem {
    private static final Logger LOGGER = LoggerFactory.getLogger(LocalFileSystem.class);
    private File workingDir = new File(System.getProperty("user.dir")).getAbsoluteFile();
    TreeMap<File, FileInputStream> sharedLockDataSet = new TreeMap<File, FileInputStream>();
    TreeMap<File, FileOutputStream> nonsharedLockDataSet = new TreeMap<File, FileOutputStream>();
    TreeMap<File, FileLock> lockObjSet = new TreeMap<File, FileLock>();
    /**
     * 默认情况下使用复制/删除而不是重命名
     */
    boolean useCopyForRename = true;

    /**
     * 构造一个本地文件系统客户机。
     * @param conf
     */
    public LocalFileSystem(Configuration conf) {
        super(conf);
        // 如果您发现一个OS可靠地支持跨文件系统/卷的非posix rename(2)，可以取消注释。
        // String os = System.getProperty("os.name");
        // if (os.toLowerCase().indexOf("os-with-super-rename") != -1)
        //     useCopyForRename = false;
    }

    /**
     * 如果文件存在，返回1x1 'localhost'单元格。
     * 否则返回null。
     * @param file
     * @param start 没用
     * @param len   没用
     * @return
     * @throws IOException
     */
    @Override
    public String[][] getFileCacheHints(File file, long start, long len) {
        if (!file.exists()) {
            return null;
        } else {
            String[][] result = new String[1][];
            result[0] = new String[1];
            result[0][0] = "localhost";
            return result;
        }
    }

    @Override
    public String getName() {
        return "local";
    }

    /*******************************************************
     * For open()'s FSInputStream
     *******************************************************/
    class LocalFSFileInputStream extends FSInputStream {
        FileInputStream fis;

        public LocalFSFileInputStream(File f) throws IOException {
            this.fis = new FileInputStream(f);
        }

        @Override
        public void seek(long pos) throws IOException {
            fis.getChannel().position(pos);
        }

        @Override
        public long getPos() throws IOException {
            return fis.getChannel().position();
        }

        /*
         * 转发给fis
         */
        @Override
        public int available() throws IOException {
            return fis.available();
        }

        @Override
        public void close() throws IOException {
            fis.close();
        }

        public boolean markSupport() {
            return false;
        }

        @Override
        public int read() {
            try {
                return fis.read();
            } catch (IOException e) {
                // 意想不到的异常
                // 假设本机fs错误
                throw new FSError(e);
            }
        }

        @Override
        public int read(byte[] b, int off, int len) {
            try {
                return fis.read(b, off, len);
            } catch (IOException e) {
                // 意想不到的异常
                // 假设本机fs错误
                throw new FSError(e);
            }
        }

        @Override
        public long skip(long n) throws IOException {
            return fis.skip(n);
        }
    }

    @Override
    public FSInputStream openRaw(File f) throws IOException {
        f = makeAbsolute(f);
        if (!f.exists()) {
            throw new FileNotFoundException(f.toString());
        }
        return new LocalFSFileInputStream(f);
    }

    /*********************************************************
     * For create()'s FSOutputStream.
     *********************************************************/
    class LocalFSFileOutputStream extends FSOutputStream {
        FileOutputStream fos;

        public LocalFSFileOutputStream(File f) throws IOException {
            this.fos = new FileOutputStream(f);
        }

        @Override
        public long getPos() throws IOException {
            return fos.getChannel().position();
        }

        /*
         * 转发给fos
         */
        @Override
        public void close() throws IOException {
            fos.close();
        }

        @Override
        public void flush() throws IOException {
            fos.flush();
        }

        @Override
        public void write(byte[] b, int off, int len) throws IOException {
            try {
                fos.write(b, off, len);
            } catch (IOException e) {
                // 意想不到的异常
                // 假设本机fs错误
                throw new FSError(e);
            }
        }

        @Override
        public void write(int b) throws IOException {
            try {
                fos.write(b);
            } catch (IOException e) {
                // 意想不到的异常
                // 假设本机fs错误
                throw new FSError(e);
            }
        }
    }

    private File makeAbsolute(File f) {
        if (isAbsolute(f)) {
            return f;
        } else {
            return new File(workingDir, f.toString()).getAbsoluteFile();
        }
    }

    @Override
    public FSOutputStream createRaw(File f, boolean overwrite) throws IOException {
        f = makeAbsolute(f);
        if (f.exists() && !overwrite) {
            throw new IOException("File already exists:" + f);
        }
        File parent = f.getParentFile();
        if (parent != null) {
            parent.mkdirs();
        }
        return new LocalFSFileOutputStream(f);
    }

    @Override
    public boolean renameRaw(File src, File dst) throws IOException {
        src = makeAbsolute(src);
        dst = makeAbsolute(dst);
        if (useCopyForRename) {
            FileUtil.copyContents(this, src, dst, true, getConf());
            return fullyDelete(src);
        } else {
            return src.renameTo(dst);
        }
    }

    @Override
    public boolean deleteRaw(File f) throws IOException {
        f = makeAbsolute(f);
        if (f.isFile()) {
            return f.delete();
        } else {
            return fullyDelete(f);
        }
    }

    @Override
    public boolean exists(File f) {
        f = makeAbsolute(f);
        return f.exists();
    }

    @Override
    public boolean isDirectory(File f) {
        f = makeAbsolute(f);
        return f.isDirectory();
    }

    @Override
    public boolean isAbsolute(File f) {
        return f.isAbsolute() ||
                f.getPath().startsWith("/") ||
                f.getPath().startsWith("\\");
    }

    @Override
    public long getLength(File f) throws IOException {
        f = makeAbsolute(f);
        return f.length();
    }

    @Override
    public File[] listFilesRaw(File f) throws IOException {
        f = makeAbsolute(f);
        return f.listFiles();
    }

    @Override
    public void mkdirs(File f) throws IOException {
        f = makeAbsolute(f);
        f.mkdirs();
    }

    /**
     * 将工作目录设置为给定目录。
     * 同时设置局部变量和系统属性。
     * 注意，只有当应用程序明确地调用java.io.File.getAbsolutePath()时，才使用系统属性。
     * @param new_dir
     */
    @Override
    public void setWorkingDirectory(File new_dir) {
        workingDir = makeAbsolute(new_dir);
    }

    @Override
    public File getWorkingDirectory() {
        return workingDir;
    }

    @Override
    public synchronized void lock(File f, boolean shared) throws IOException {
        f = makeAbsolute(f);
        f.createNewFile();

        FileLock lockObj = null;
        if (shared) {
            FileInputStream lockData = new FileInputStream(f);
            lockObj = lockData.getChannel().lock(0L, Long.MAX_VALUE, shared);
            sharedLockDataSet.put(f, lockData);
        } else {
            FileOutputStream lockData = new FileOutputStream(f);
            lockObj = lockData.getChannel().lock(0L, Long.MAX_VALUE, shared);
            nonsharedLockDataSet.put(f, lockData);
        }
        lockObjSet.put(f, lockObj);
    }

    @Override
    public synchronized void release(File f) throws IOException {
        f = makeAbsolute(f);
        FileLock lockObj = lockObjSet.get(f);
        FileInputStream sharedLockData = sharedLockDataSet.get(f);
        FileOutputStream nonsharedLockData = nonsharedLockDataSet.get(f);

        if (lockObj == null) {
            throw new IOException("Given target not held as lock");
        }
        if (sharedLockData == null && nonsharedLockData == null) {
            throw new IOException("Given target not held as lock");
        }

        lockObj.release();
        lockObjSet.remove(f);
        if (sharedLockData != null) {
            sharedLockData.close();
            sharedLockDataSet.remove(f);
        } else {
            nonsharedLockData.close();
            nonsharedLockDataSet.remove(f);
        }
    }

    /**
     * 对于本地文件系统，我们可以重命名文件。
     * @param src
     * @param dst
     * @throws IOException
     */
    @Override
    public void moveFromLocalFile(File src, File dst) throws IOException {
        if (!src.equals(dst)) {
            src = makeAbsolute(src);
            dst = makeAbsolute(dst);
            if (useCopyForRename) {
                FileUtil.copyContents(this, src, dst, true, getConf());
                fullyDelete(src);
            } else {
                src.renameTo(dst);
            }
        }
    }

    /**
     * 与moveFromLocalFile()类似，只是源代码保持不变。
     * @param src
     * @param dst
     * @throws IOException
     */
    @Override
    public void copyFromLocalFile(File src, File dst) throws IOException {
        if (!src.equals(dst)) {
            src = makeAbsolute(src);
            dst = makeAbsolute(dst);
            FileUtil.copyContents(this, src, dst, true, getConf());
        }
    }

    /**
     * 在这种情况下，我们不能删除src文件。太糟糕了。
     * @param src
     * @param dst
     * @throws IOException
     */
    @Override
    public void copyToLocalFile(File src, File dst) throws IOException {
        if (!src.equals(dst)) {
            src = makeAbsolute(src);
            dst = makeAbsolute(dst);
            FileUtil.copyContents(this, src, dst, true, getConf());
        }
    }

    /**
     * 我们可以直接将输出写入最终位置
     * @param fsOutputFile
     * @param tmpLocalFile
     * @return
     */
    @Override
    public File startLocalOutput(File fsOutputFile, File tmpLocalFile) {
        return makeAbsolute(fsOutputFile);
    }

    /**
     * 它在正确的地方——没事可做。
     * @param fsWorkingFile
     * @param tmpLocalFile
     */
    @Override
    public void completeLocalOutput(File fsWorkingFile, File tmpLocalFile) {
    }

    /**
     * 我们可以直接从实际的本地fs中读取。
     * @param fsInputFile
     * @param tmpLocalFile
     * @return
     */
    @Override
    public File startLocalInput(File fsInputFile, File tmpLocalFile) {
        return makeAbsolute(fsInputFile);
    }

    /**
     * 做完了阅读。没什么清理的。
     * @param localFile
     */
    @Override
    public void completeLocalInput(File localFile) {
        // 忽略文件，它在正确的目的地!
    }

    @Override
    public void close() throws IOException {
    }

    @Override
    public String toString() {
        return "LocalFS";
    }

    /**
     * 实现我们自己的版本，而不是使用FileUtil中的版本，以避免无限递归。
     * @param dir
     * @return
     */
    private boolean fullyDelete(File dir) {
        dir = makeAbsolute(dir);
        File[] contents = dir.listFiles();
        if (contents != null) {
            for (int i = 0; i < contents.length; i++) {
                if (contents[i].isFile()) {
                    if (!contents[i].delete()) {
                        return false;
                    }
                } else {
                    if (!fullyDelete(contents[i])) {
                        return false;
                    }
                }
            }
        }
        return dir.delete();
    }

    /**
     * 将文件移动到同一设备上的一个坏文件目录，这样它们的存储就不会被重用。
     * @param f
     * @param in     打开的文件流
     * @param start  文件中坏数据开始的位置
     * @param length 文件中坏数据的长度
     * @param crc    数据的预期CRC32
     */
    @Override
    public void reportChecksumFailure(File f, FSInputStream in, long start, long length, int crc) {
        try {
            // 规范化
            f = makeAbsolute(f).getCanonicalFile();
            // 在同一设备上找到f的最高可写父目录
            String device = DFFactory.getDF(f.toString(), getConf()).getMount();
            File parent = f.getParentFile();
            File dir;
            do {
                dir = parent;
                parent = parent.getParentFile();
            } while (parent.canWrite() && parent.toString().startsWith(device));
            // 把文件移到那里
            File badDir = new File(dir, "bad_files");
            badDir.mkdirs();
            String suffix = "." + new Random().nextInt();
            File badFile = new File(badDir, f.getName() + suffix);
            LOGGER.warn("Moving bad file " + f + " to " + badFile);
            in.close();                               // close it first
            f.renameTo(badFile);                      // rename it
            // move checksum file too
            File checkFile = getChecksumFile(f);
            checkFile.renameTo(new File(badDir, checkFile.getName() + suffix));
        } catch (IOException e) {
            LOGGER.warn("Error moving bad file " + f + ": " + e);
        }
    }

    @Override
    public long getBlockSize() {
        // 默认为32MB:足够大，可以最小化seek的影响
        return Constants.FS_LOCAL_BLOCK_SIZE;
    }

}
