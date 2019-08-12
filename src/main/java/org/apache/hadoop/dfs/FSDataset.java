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
import java.util.*;

import org.apache.hadoop.fs.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.df.DF;
import org.apache.hadoop.fs.df.DFFactory;

/**
 * FSDataset管理一组数据块。<br/>
 * 每个块在磁盘上都有一个惟一的名称和一个区段。
 * @author 章云
 * @date 2019/8/9 15:18
 */
public class FSDataset implements FSConstants {
    static final double USABLE_DISK_PCT = 0.98;

    /**
     * 一种节点类型，可以构建成反映本地磁盘上块层次结构的树。
     */
    public class FSDir {
        File dir;
        FSDir[] children;

        public FSDir(File dir) {
            this.dir = dir;
            this.children = null;
        }

        public File getDirName() {
            return dir;
        }

        public FSDir[] getChildren() {
            return children;
        }

        public void addBlock(Block b, File src) {
            addBlock(b, src, b.getBlockId(), 0);
        }

        void addBlock(Block b, File src, long blkid, int depth) {
            // 如果没有子目录，则添加到本地目录
            if (children == null) {
                src.renameTo(new File(dir, b.getBlockName()));
                //测试这个目录的内容是否应该分解为子目录。
                //提醒- mjc -不久的将来，我们将需要这个代码
                //工作。它阻止了数据锁的运行
                //进入一个巨大的目录。
                /**
                 File localFiles[] = dir.listFiles();
                 if (localFiles.length == 16) {
                 //
                 // 创建所有必要的子目录
                 //
                 this.children = new FSDir[16];
                 for (int i = 0; i < children.length; i++) {
                 String str = Integer.toBinaryString(i);
                 try {
                 File subdir = new File(dir, "dir_" + str);
                 subdir.mkdir();
                 children[i] = new FSDir(subdir);
                 } catch (StringIndexOutOfBoundsException excep) {
                 excep.printStackTrace();
                 System.out.println("Ran into problem when i == " + i + " an str = " + str);
                 }
                 }

                 //
                 // 将现有文件移动到新的目录中
                 //
                 for (int i = 0; i < localFiles.length; i++) {
                 Block srcB = new Block(localFiles[i]);
                 File dst = getBlockFilename(srcB, blkid, depth);
                 if (!src.renameTo(dst)) {
                 System.out.println("Unexpected problem in renaming " + src);
                 }
                 }
                 }
                 **/
            } else {
                // 发现子目录
                children[getHalfByte(blkid, depth)].addBlock(b, src, blkid, depth + 1);
            }
        }

        /**
         * 用在此节点上找到的任何子块填充给定的块集。
         */
        public void getBlockInfo(TreeSet<Block> blockSet) {
            if (children != null) {
                for (int i = 0; i < children.length; i++) {
                    children[i].getBlockInfo(blockSet);
                }
            }
            File[] blockFiles = dir.listFiles();
            for (int i = 0; i < blockFiles.length; i++) {
                if (Block.isBlockFilename(blockFiles[i])) {
                    blockSet.add(new Block(blockFiles[i], blockFiles[i].length()));
                }
            }
        }

        /**
         * 找到对应于给定块的文件
         */
        public File getBlockFilename(Block b) {
            return getBlockFilename(b, b.getBlockId(), 0);
        }

        /**
         * 帮助器方法查找块的文件
         */
        private File getBlockFilename(Block b, long blkid, int depth) {
            if (children == null) {
                return new File(dir, b.getBlockName());
            } else {
                //从深度开始提升4位，向左->向右。
                //这意味着有2^4个可能的子结点，也就是16。
                //最大深度因此为((len(long) / 4) == 16)。
                return children[getHalfByte(blkid, depth)].getBlockFilename(b, blkid, depth + 1);
            }
        }

        /**
         * 返回一个包含0-15的数字。<br/>
         * 从指定的长中提取正确的半字节。
         */
        private int getHalfByte(long blkid, int halfByteIndex) {
            blkid = blkid >> ((15 - halfByteIndex) * 4);
            return (int) ((0x000000000000000F) & blkid);
        }

        @Override
        public String toString() {
            return "FSDir{" +
                    "dir=" + dir +
                    ", children=" + (children == null ? null : Arrays.asList(children)) +
                    "}";
        }
    }

    //////////////////////////////////////////////////////
    //
    // FSDataSet
    //
    //////////////////////////////////////////////////////

    DF diskUsage;
    File data = null, tmp = null;
    long reserved = 0;
    FSDir dirTree;
    TreeSet ongoingCreates = new TreeSet();

    /**
     * FSDataset有一个目录，它在其中加载数据文件。
     */
    public FSDataset(File dir, Configuration conf) throws IOException {
        diskUsage = DFFactory.getDF(dir.getCanonicalPath(), conf);
        this.data = new File(dir, "data");
        if (!data.exists()) {
            data.mkdirs();
        }
        this.tmp = new File(dir, "tmp");
        if (tmp.exists()) {
            FileUtil.fullyDelete(tmp, conf);
        }
        this.tmp.mkdirs();
        this.dirTree = new FSDir(data);
    }

    /**
     * 返回使用和未使用的总容量
     */
    public long getCapacity() throws IOException {
        return diskUsage.getCapacity();
    }

    /**
     * 返回在FSDataset中还可以存储多少字节
     */
    public long getRemaining() throws IOException {
        return (Math.round(USABLE_DISK_PCT * diskUsage.getAvailable())) - reserved;
    }

    /**
     * 找出块在磁盘上的长度
     */
    public long getLength(Block b) throws IOException {
        if (!isValidBlock(b)) {
            throw new IOException("Block " + b + " is not valid.");
        }
        File f = getFile(b);
        return f.length();
    }

    /**
     * 从指定的块中获取数据流。
     */
    public InputStream getBlockData(Block b) throws IOException {
        if (!isValidBlock(b)) {
            throw new IOException("Block " + b + " is not valid.");
        }
        return new FileInputStream(getFile(b));
    }

    /**
     * A街区b马上就要来了!
     */
    public boolean startBlock(Block b) throws IOException {
        // 确保block不是“有效的”
        if (isValidBlock(b)) {
            throw new IOException("Block " + b + " is valid, and cannot be created.");
        }
        return true;
    }

    /**
     * 开始写一个块文件
     */
    public OutputStream writeToBlock(Block b) throws IOException {
        // 确保该块不是一个有效的块——我们仍然在创建它!
        if (isValidBlock(b)) {
            throw new IOException("Block " + b + " is valid, and cannot be written to.");
        }
        // 序列化对/tmp的访问，并检查文件是否已经存在。
        File f = null;
        synchronized (ongoingCreates) {
            // 它已经在创建过程中了吗?
            if (ongoingCreates.contains(b)) {
                throw new IOException("Block " + b + " has already been started (though not completed), and thus cannot be created.");
            }
            // 检查一下我们的空间是否太小
            if (getRemaining() < BLOCK_SIZE) {
                throw new IOException("Insufficient space for an additional block");
            }
            // OK, all's well.  Register the create, adjust 'reserved' size, & create file
            ongoingCreates.add(b);
            reserved += BLOCK_SIZE;
            f = getTmpFile(b);
            try {
                if (f.exists()) {
                    throw new IOException("Unexpected problem in startBlock() for " + b + ".  File " + f + " should not be present, but is.");
                }
                // 创建零长度的临时文件
                if (!f.createNewFile()) {
                    throw new IOException("Unexpected problem in startBlock() for " + b + ".  File " + f + " should be creatable, but is already present.");
                }
            } catch (IOException ie) {
                System.out.println("Exception!  " + ie);
                ongoingCreates.remove(b);
                reserved -= BLOCK_SIZE;
                throw ie;
            }
        }
        // 最后，允许编写器对块文件进行提醒(mjc)，使其成为一个过滤器流，强制执行最大块大小，这样客户端就不会发疯
        return new FileOutputStream(f);
    }

    //提醒- mjc -最终我们应该有一个超时系统来清理被抛弃的客户端留下的块文件。
    //我们应该有一个计时器，这样，如果创建了一个块文件，但是无效，并且已经空闲了48小时，我们可以安全地进行GC。

    /**
     * 完成方块写!
     */
    public void finalizeBlock(Block b) throws IOException {
        File f = getTmpFile(b);
        if (!f.exists()) {
            throw new IOException("No temporary file " + f + " for block " + b);
        }
        synchronized (ongoingCreates) {
            // 确保仍然注册为进行中
            if (!ongoingCreates.contains(b)) {
                throw new IOException("Tried to finalize block " + b + ", but not in ongoingCreates table");
            }
            long finalLen = f.length();
            b.setNumBytes(finalLen);
            // 移动文件(提醒- mjc -耻辱移动文件在一个同步部分!也许删除?)
            dirTree.addBlock(b, f);
            // 完成，从ongoingcreate中注销
            if (!ongoingCreates.remove(b)) {
                throw new IOException("Tried to finalize block " + b + ", but could not find it in ongoingCreates after file-move!");
            }
            reserved -= BLOCK_SIZE;
        }
    }

    /**
     * 返回一个块数据表
     */
    public Block[] getBlockReport() {
        TreeSet blockSet = new TreeSet();
        dirTree.getBlockInfo(blockSet);
        Block[] blockTable = new Block[blockSet.size()];
        int i = 0;
        for (Iterator it = blockSet.iterator(); it.hasNext(); i++) {
            blockTable[i] = (Block) it.next();
        }
        return blockTable;
    }

    /**
     * 检查给定的块是否有效。
     */
    public boolean isValidBlock(Block b) {
        File f = getFile(b);
        if (f.exists()) {
            return true;
        } else {
            return false;
        }
    }

    /**
     * 我们被告知block不再有效。<br/>
     * 我们可以懒懒散散地收集垃圾，但何必费事呢?把它处理掉。
     */
    public void invalidate(Block[] invalidBlks) throws IOException {
        for (int i = 0; i < invalidBlks.length; i++) {
            File f = getFile(invalidBlks[i]);
            if (!f.delete()) {
                throw new IOException("Unexpected error trying to delete block " + invalidBlks[i] + " at file " + f);
            }
        }
    }

    /**
     * 将块标识符转换为文件名。
     */
    File getFile(Block b) {
        // 提醒- mjc -应缓存此结果的性能
        return dirTree.getBlockFilename(b);
    }

    /**
     * 如果仍在创建此块，则获取临时文件。
     */
    File getTmpFile(Block b) {
        // 提醒- mjc -应缓存此结果的性能
        return new File(tmp, b.getBlockName());
    }

    @Override
    public String toString() {
        return "FSDataset{" +
                "dirpath='" + diskUsage.getDirPath() + "'" +
                "}";
    }

}
