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

import java.io.*;
import java.net.*;
import java.util.*;

import org.apache.hadoop.dfs.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.util.Constants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 一个相当通用的文件系统的抽象基类。<br/>
 * 它可以作为分布式文件系统实现，也可以作为反映本地连接磁盘的“本地”文件系统实现。<br/>
 * 本地版本用于小型Hadopp实例和测试。<br/>
 * 所有可能使用Hadoop分布式文件系统的用户代码都应该编写为使用文件系统对象。<br/>
 * Hadoop DFS是一个多机器系统，它以单个磁盘的形式出现。<br/>
 * 这是有用的，因为它的容错能力和潜在的非常大的容量。<br/>
 * 本地实现是{@link LocalFileSystem}，分布式实现是{@link DistributedFileSystem}。
 * @author 章云
 * @date 2019/8/2 14:00
 */
public abstract class FileSystem extends Configured {
    private static final Logger LOGGER = LoggerFactory.getLogger(FileSystem.class);

    private static final HashMap<String, FileSystem> NAME_TO_FS = new HashMap<String, FileSystem>();

    /**
     * 解析cmd行参数，从i开始。从数组中删除已使用的参数。我们期望参数的形式:'-local | -dfs <namenode:port>' <br/>
     * 以后使用fs.default.name配置选项（已过时）
     * @param argv
     * @param i
     * @param conf
     * @return
     * @throws IOException
     */
    public static FileSystem parseArgs(String[] argv, int i, Configuration conf) throws IOException {
        if (argv.length - i < 1) {
            throw new IOException("Must indicate filesystem type for DFS");
        }
        int orig = i;
        FileSystem fs = null;
        String cmd = argv[i];
        if ("-dfs".equals(cmd)) {
            // 分布式文件系统
            i++;
            InetSocketAddress addr = DataNode.createSocketAddr(argv[i++]);
            fs = new DistributedFileSystem(addr, conf);
        } else if ("-local".equals(cmd)) {
            // 本地文件系统
            i++;
            fs = new LocalFileSystem(conf);
        } else {
            // 使用fs.default.name配置选项
            fs = get(conf);                          // using default
            LOGGER.info("No FS indicated, using default:" + fs.getName());

        }
        System.arraycopy(argv, i, argv, orig, argv.length - i);
        for (int j = argv.length - i; j < argv.length; j++) {
            argv[j] = null;
        }
        return fs;
    }

    /**
     * 返回配置的文件系统实现。
     * @param conf
     * @return
     * @throws IOException
     */
    public static FileSystem get(Configuration conf) throws IOException {
        return getNamed(Constants.FS_DEFAULT_NAME, conf);
    }

    /**
     * 返回此文件系统的名称，适合传递给{@link FileSystem #getNamed(String, Configuration)}。
     * @return
     */
    public abstract String getName();

    /**
     * 返回指定的文件系统。名称可以是字符串“local”，也可以是主机:端口对，用于命名DFS名称服务器。
     * @param name
     * @param conf
     * @return
     * @throws IOException
     */
    public static FileSystem getNamed(String name, Configuration conf) {
        FileSystem fs = NAME_TO_FS.get(name);
        if (fs == null) {
            if ("local".equals(name)) {
                fs = new LocalFileSystem(conf);
            } else {
                fs = new DistributedFileSystem(DataNode.createSocketAddr(name), conf);
            }
            NAME_TO_FS.put(name, fs);
        }
        return fs;
    }

    /**
     * 返回与文件关联的校验文件的名称。
     * @param file
     * @return
     */
    public File getChecksumFile(File file) {
        return new File(file.getParentFile(), "." + file.getName() + ".crc");
    }

    /**
     * 如果文件是校验文件名，返回true。
     * @param file
     * @return
     */
    public static boolean isChecksumFile(File file) {
        String name = file.getName();
        return name.startsWith(".") && name.endsWith(".crc");
    }

    ///////////////////////////////////////////////////////////////
    // 文件系统
    ///////////////////////////////////////////////////////////////

    protected FileSystem(Configuration conf) {
        super(conf);
    }

    /**
     * 返回一个大小为1x1或更大的2D数组，其中包含可以找到给定文件部分内容的主机名。<br/>
     * 对于不存在的文件或区域，将返回null。<br/>
     * 这个调用对DFS最有帮助，它返回包含给定文件的机器的主机名。<br/>
     * 文件系统将简单地返回一个包含“localhost”的名字。
     * @param file
     * @param start
     * @param len
     * @return
     * @throws IOException
     */
    public abstract String[][] getFileCacheHints(File file, long start, long len) throws IOException;

    /**
     * 在指定的文件上打开FSDataInputStream。
     * @param file       要打开的文件名
     * @param bufferSize 要使用的缓冲区的大小。
     * @return
     * @throws IOException
     */
    public FSDataInputStream open(File file, int bufferSize) throws IOException {
        return new FSDataInputStream(this, file, bufferSize, getConf());
    }

    /**
     * 在指定的文件上打开FSDataInputStream。
     * @param file 要打开的文件名
     * @return
     * @throws IOException
     */
    public FSDataInputStream open(File file) throws IOException {
        return new FSDataInputStream(this, file, getConf());
    }

    /**
     * 为指定的文件打开InputStream，无论是本地文件还是通过DFS。
     * @param file
     * @return
     * @throws IOException
     */
    public abstract FSInputStream openRaw(File file) throws IOException;

    /**
     * 在指定的文件上打开FSDataOutputStream。<br/>
     * 默认情况下文件被覆盖。
     * @param file 要打开的文件
     * @return
     * @throws IOException
     */
    public FSDataOutputStream create(File file) throws IOException {
        return create(file, true, Constants.IO_FILE_BUFFER_SIZE);
    }

    /**
     * 在指定的文件上打开FSDataOutputStream。
     * @param file       要打开的文件
     * @param overwrite  如果该名称的文件已经存在，那么如果为真，将覆盖该文件，如果为假，将抛出错误。
     * @param bufferSize 要使用的缓冲区的大小。
     * @return
     * @throws IOException
     */
    public FSDataOutputStream create(File file, boolean overwrite, int bufferSize) throws IOException {
        return new FSDataOutputStream(this, file, overwrite, getConf(), bufferSize);
    }

    /**
     * 在指定的文件处打开OutputStream。
     * @param file      要打开的文件
     * @param overwrite 如果该名称的文件已经存在，那么如果为真，将覆盖该文件，如果为假，将抛出错误。
     * @return
     * @throws IOException
     */
    public abstract FSOutputStream createRaw(File file, boolean overwrite) throws IOException;

    /**
     * 将给定文件创建为一个全新的零长度文件。<br/>
     * 如果create失败，或者它已经存在，返回false。
     * @param file
     * @return
     * @throws IOException
     */
    public boolean createNewFile(File file) throws IOException {
        if (exists(file)) {
            return false;
        } else {
            OutputStream out = createRaw(file, false);
            out.close();
            return true;
        }
    }

    /**
     * 将文件src重命名为文件dst。可以在本地fs或远程DFS上发生。
     * @param src
     * @param dst
     * @return
     * @throws IOException
     */
    public boolean rename(File src, File dst) throws IOException {
        if (isDirectory(src)) {
            return renameRaw(src, dst);
        } else {
            boolean value = renameRaw(src, dst);
            File checkFile = getChecksumFile(src);
            if (exists(checkFile)) {
                // 尝试重命名校验文件
                renameRaw(checkFile, getChecksumFile(dst));
            }
            return value;
        }

    }

    /**
     * 将文件src重命名为文件dst。可以在本地fs或远程DFS上发生。
     * @param src
     * @param dst
     * @return
     * @throws IOException
     */
    public abstract boolean renameRaw(File src, File dst) throws IOException;

    /**
     * 删除文件
     * @param file
     * @return
     * @throws IOException
     */
    public boolean delete(File file) throws IOException {
        if (isDirectory(file)) {
            return deleteRaw(file);
        } else {
            // 尝试删除校验文件
            deleteRaw(getChecksumFile(file));
            return deleteRaw(file);
        }
    }

    /**
     * 删除文件
     * @param file
     * @return
     * @throws IOException
     */
    public abstract boolean deleteRaw(File file) throws IOException;

    /**
     * 校验文件是否存在
     * @param file
     * @return
     * @throws IOException
     */
    public abstract boolean exists(File file) throws IOException;

    /**
     * 如果指定的路径是目录，则为True。
     * @param file
     * @return
     * @throws IOException
     */
    public abstract boolean isDirectory(File file) throws IOException;

    /**
     * 如果指定的路径是常规文件，则为True。
     * @param file
     * @return
     * @throws IOException
     */
    public boolean isFile(File file) throws IOException {
        if (exists(file) && !isDirectory(file)) {
            return true;
        } else {
            return false;
        }
    }

    /**
     * 如果指定的路径是绝对路径，则为True。
     * @param file
     * @return
     */
    public abstract boolean isAbsolute(File file);

    /**
     * 文件中的字节数。
     * @param file
     * @return
     * @throws IOException
     */
    public abstract long getLength(File file) throws IOException;

    /**
     * 列出目录中的文件。
     * @param file
     * @return
     * @throws IOException
     */
    public File[] listFiles(File file) throws IOException {
        return listFiles(file, new FileFilter() {
            @Override
            public boolean accept(File file) {
                // 只接受非校验文件
                return !isChecksumFile(file);
            }
        });
    }

    /**
     * 列出目录中的文件。
     * @param file
     * @return
     * @throws IOException
     */
    public abstract File[] listFilesRaw(File file) throws IOException;

    /**
     * 过滤目录中的文件。
     * @param file
     * @param filter
     * @return
     * @throws IOException
     */
    public File[] listFiles(File file, FileFilter filter) throws IOException {
        Vector<File> results = new Vector<File>();
        File[] listing = listFilesRaw(file);
        if (listing != null) {
            for (int i = 0; i < listing.length; i++) {
                if (filter.accept(listing[i])) {
                    results.add(listing[i]);
                }
            }
        }
        return results.toArray(new File[results.size()]);
    }

    /**
     * 为给定的文件系统设置当前工作目录。<br/>
     * 所有相对路径都将相对于它进行解析。
     * @param new_dir
     */
    public abstract void setWorkingDirectory(File new_dir);

    /**
     * 获取给定文件系统的当前工作目录
     * @return 目录路径名
     */
    public abstract File getWorkingDirectory();

    /**
     * 创建给定的文件，可以创建多级目录
     * @param file
     * @throws IOException
     */
    public abstract void mkdirs(File file) throws IOException;

    /**
     * 获取给定文件上的锁
     * @param file
     * @param shared
     * @throws IOException
     */
    public abstract void lock(File file, boolean shared) throws IOException;

    /**
     * 释放锁
     * @param file
     * @throws IOException
     */
    public abstract void release(File file) throws IOException;

    /**
     * src文件位于本地磁盘上。<br/>
     * 以给定的dst名称将其添加到FS中，然后原文件将保持完整
     * @param src
     * @param dst
     * @throws IOException
     */
    public abstract void copyFromLocalFile(File src, File dst) throws IOException;

    /**
     * src文件位于本地磁盘上。<br/>
     * 以给定的dst名称将其添加到FS，然后删除原文件。
     * @param src
     * @param dst
     * @throws IOException
     */
    public abstract void moveFromLocalFile(File src, File dst) throws IOException;

    /**
     * src文件位于FS2之下，dst位于本地磁盘上。<br/>
     * 将它从FS控件复制到本地dst名称。
     * @param src
     * @param dst
     * @throws IOException
     */
    public abstract void copyToLocalFile(File src, File dst) throws IOException;

    // 没有实现的
    // public abstract void moveToLocalFile(File src, File dst) throws IOException;

    /**
     * 与copyToLocalFile(文件src，文件dst)相同，只是后面会删除源文件。<br/>
     * 返回用户可以写入输出的本地文件。<br/>
     * 调用者提供最终的FS目标名称和本地工作文件。<br/>
     * 如果FS是本地的，我们直接写入目标。<br/>
     * 如果FS是远程的，我们写入tmp本地区域。
     * @param fsOutputFile
     * @param tmpLocalFile
     * @return
     * @throws IOException
     */
    public abstract File startLocalOutput(File fsOutputFile, File tmpLocalFile) throws IOException;

    /**
     * 当我们完成对目标的写入时调用。<br/>
     * 一个本地FS什么也做不了，因为我们已经写到了正确的位置。<br/>
     * 远程FS将tmpLocalFile的内容复制到fsOutputFile的正确目标。
     * @param fsOutputFile
     * @param tmpLocalFile
     * @throws IOException
     */
    public abstract void completeLocalOutput(File fsOutputFile, File tmpLocalFile) throws IOException;

    /**
     * 返回用户可以从中读取的本地文件。<br/>
     * 调用者提供最终的FS目标名称和本地工作文件。<br/>
     * 如果FS是本地的，则直接从源读取。<br/>
     * 如果FS是远程的，我们将数据写入tmp本地区域。
     * @param fsInputFile
     * @param tmpLocalFile
     * @return
     * @throws IOException
     */
    public abstract File startLocalInput(File fsInputFile, File tmpLocalFile) throws IOException;

    /**
     * 当我们完成对目标的写入时调用。<br/>
     * 一个本地FS什么也做不了，因为我们已经写到了正确的位置。<br/>
     * 远程FS将tmpLocalFile的内容复制到fsOutputFile的正确目标。
     * @param localFile
     * @throws IOException
     */
    public abstract void completeLocalInput(File localFile) throws IOException;

    /**
     * 不再需要文件系统操作。<br/>
     * 将释放任何持有的锁。
     * @throws IOException
     */
    public abstract void close() throws IOException;

    /**
     * 向文件系统报告校验和错误。
     * @param file   包含错误的文件名
     * @param in     打开的文件流
     * @param start  文件中坏数据开始的位置
     * @param length 文件中坏数据的长度
     * @param crc    数据的预期CRC32
     */
    public abstract void reportChecksumFailure(File file, FSInputStream in, long start, long length, int crc);

    /**
     * 返回大输入文件的最佳分割字节数，以最小化i/o时间。
     * @return
     */
    public abstract long getBlockSize();

}
