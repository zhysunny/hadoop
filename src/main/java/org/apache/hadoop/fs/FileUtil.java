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
import org.apache.hadoop.util.Constants;

import java.io.File;
import java.io.IOException;

/**
 * 文件处理util方法的集合
 * @author 章云
 * @date 2019/8/2 15:21
 */
public class FileUtil {
    /**
     * 删除目录及其所有内容。默认本地文件系统<br/>
     * 如果返回false，目录可能被部分删除。
     * @param dir
     * @param conf
     * @return
     * @throws IOException
     */
    public static boolean fullyDelete(File dir, Configuration conf) throws IOException {
        return fullyDelete(new LocalFileSystem(conf), dir);
    }

    public static boolean fullyDelete(FileSystem fs, File dir) throws IOException {
//        当前fs.detele(File)对于LocalFileSystem.java和DistributedFileSystem.java都意味着完全删除。
//        如果将来实现发生变化，也应该对其进行修改。
        return fs.delete(dir);
    }

    /**
     * 将文件的内容复制到新位置。
     * 返回目标文件是否被覆盖
     * 如果返回false，目录可能被部分拷贝。
     * @param fs
     * @param src
     * @param dst
     * @param overwrite
     * @param conf
     * @return
     * @throws IOException
     */
    public static boolean copyContents(FileSystem fs, File src, File dst, boolean overwrite, Configuration conf) throws IOException {
        if (fs.exists(dst) && !overwrite) {
            return false;
        }
        File dstParent = dst.getParentFile();
        if ((dstParent != null) && (!fs.exists(dstParent))) {
            fs.mkdirs(dstParent);
        }
        if (fs.isFile(src)) {
            FSInputStream in = fs.openRaw(src);
            // 拷贝文件
            try {
                FSOutputStream out = fs.createRaw(dst, true);
                byte buf[] = new byte[Constants.IO_FILE_BUFFER_SIZE];
                try {
                    int readBytes = in.read(buf);
                    while (readBytes >= 0) {
                        out.write(buf, 0, readBytes);
                        readBytes = in.read(buf);
                    }
                } finally {
                    out.close();
                }
            } finally {
                in.close();
            }
        } else {
            fs.mkdirs(dst);
            File contents[] = fs.listFilesRaw(src);
            if (contents != null) {
                for (int i = 0; i < contents.length; i++) {
                    File newDst = new File(dst, contents[i].getName());
                    if (!copyContents(fs, contents[i], newDst, overwrite, conf)) {
                        return false;
                    }
                }
            }
        }
        return true;
    }

    /**
     * 复制一个文件和/或目录及其所有内容(无论是数据还是其他文件/目录)
     * @param fs
     * @param src
     * @param dst
     * @param conf
     * @throws IOException
     */
    public static void recursiveCopy(FileSystem fs, File src, File dst, Configuration conf) throws IOException {
        if (fs.exists(dst) && fs.isDirectory(dst)) {
            dst = new File(dst, src.getName());
        } else if (fs.exists(dst)) {
            throw new IOException("Destination " + dst + " already exists");
        }
        // 复制项目
        if (!fs.isDirectory(src)) {
            // 如果源文件是文件，那么只需复制内容
            copyContents(fs, src, dst, true, conf);
        } else {
            // 如果源文件是dir，那么我们需要复制所有子文件。
            fs.mkdirs(dst);
            File[] contents = fs.listFiles(src);
            for (int i = 0; i < contents.length; i++) {
                recursiveCopy(fs, contents[i], new File(dst, contents[i].getName()), conf);
            }
        }
    }
}
