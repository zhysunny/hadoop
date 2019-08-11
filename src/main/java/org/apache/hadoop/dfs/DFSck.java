/**
 * Copyright 2006 The Apache Software Foundation
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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSOutputStream;
import org.apache.hadoop.io.UTF8;
import org.apache.hadoop.util.Constants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.ArrayList;
import java.util.Random;
import java.util.TreeSet;

/**
 * 该类提供了对DFS卷进行错误和次优条件的基本检查。
 * <p>工具扫描所有文件和目录，从指定的根路径开始。检测并处理以下异常情况:</p>
 * <ul>
 * <li>包含从所有datanode中完全丢失的块的文件。<br/>
 * 在这种情况下，工具可以执行以下操作之一:
 * <ul>
 * <li>没有(@link #FIXING_NONE)</li>
 * <li>将损坏的文件移动到DFS上的/lost+found目录({@link #FIXING_MOVE})。其余数据块保存为块链，表示最长的连续有效块序列。</li>
 * <li>删除损坏的文件({@link #FIXING_DELETE})</li>
 * </ul>
 * </li>
 * <li>检测具有复制不足或过度复制块的文件</li>
 * </ul>
 * 此外，该工具收集详细的总体DFS统计信息，还可以选择打印每个文件的块位置和复制因子的详细统计信息。
 * @author 章云
 * @date 2019/8/8 21:08
 */
public class DFSck {
    private static final Logger LOGGER = LoggerFactory.getLogger(DFSck.class);

    /**
     * 不要尝试任何修复。
     */
    public static final int FIXING_NONE = 0;
    /**
     * 将损坏的文件移动到/lost+found。
     */
    public static final int FIXING_MOVE = 1;
    /**
     * 删除损坏的文件。
     */
    public static final int FIXING_DELETE = 2;

    private DFSClient dfs;
    private UTF8 lostFound = null;
    private boolean lfInited = false;
    private boolean lfInitedOk = false;
    private Configuration conf;
    private boolean showFiles;
    private boolean showBlocks;
    private boolean showLocations;
    private int fixing;

    /**
     * 文件系统检查。
     * @param conf          当前配置类
     * @param fixing        预定义值之一
     * @param showFiles     显示每个被选中的文件
     * @param showBlocks    对于选中的每个文件，显示其块信息
     * @param showLocations 对于每个文件中的每个块，显示块的位置
     * @throws Exception
     */
    public DFSck(Configuration conf, int fixing, boolean showFiles, boolean showBlocks, boolean showLocations) throws Exception {
        this.conf = conf;
        this.fixing = fixing;
        this.showFiles = showFiles;
        this.showBlocks = showBlocks;
        this.showLocations = showLocations;
        String fsName = Constants.FS_DEFAULT_NAME;
        if ("local".equals(fsName)) {
            throw new Exception("This tool only checks DFS, but your config uses 'local' FS.");
        }
        this.dfs = new DFSClient(DataNode.createSocketAddr(fsName), conf);
    }

    /**
     * 检查DFS上的文件，从指定的路径开始。
     * @param path 起点
     * @return 检查的结果
     * @throws Exception
     */
    public Result fsck(String path) throws Exception {
        DFSFileInfo[] files = dfs.listFiles(new UTF8(path));
        Result res = new Result();
        res.setReplication(Constants.DFS_REPLICATION);
        for (int i = 0; i < files.length; i++) {
            check(files[i], res);
        }
        return res;
    }

    private void check(DFSFileInfo file, Result res) throws Exception {
        if (file.isDir()) {
            if (showFiles) {
                System.out.println(file.getPath() + " <dir>");
            }
            res.totalDirs++;
            DFSFileInfo[] files = dfs.listFiles(new UTF8(file.getPath()));
            for (int i = 0; i < files.length; i++) {
                check(files[i], res);
            }
            return;
        }
        res.totalFiles++;
        res.totalSize += file.getLen();
        LocatedBlock[] blocks = dfs.namenode.open(file.getPath());
        res.totalBlocks += blocks.length;
        if (showFiles) {
            System.out.print(file.getPath() + " " + file.getLen() + ", " + blocks.length + " block(s): ");
        } else {
            System.out.print('.');
            System.out.flush();
            if (res.totalFiles % 100 == 0) {
                System.out.println();
            }
        }
        int missing = 0;
        long missize = 0;
        StringBuffer report = new StringBuffer();
        for (int i = 0; i < blocks.length; i++) {
            Block block = blocks[i].getBlock();
            long id = block.getBlockId();
            DatanodeInfo[] locs = blocks[i].getLocations();
            if (locs.length > res.replication) {
                res.overReplicatedBlocks += (locs.length - res.replication);
            }
            if (locs.length < res.replication && locs.length > 0) {
                res.underReplicatedBlocks += (res.replication - locs.length);
            }
            report.append(i + ". " + id + " len=" + block.getNumBytes());
            if (locs == null || locs.length == 0) {
                report.append(" MISSING!");
                res.addMissing(block.getBlockName(), block.getNumBytes());
                missing++;
                missize += block.getNumBytes();
            } else {
                report.append(" repl=" + locs.length);
                if (showLocations) {
                    StringBuffer sb = new StringBuffer("[");
                    for (int j = 0; j < locs.length; j++) {
                        if (j > 0) {
                            sb.append(", ");
                        }
                        sb.append(locs[j]);
                    }
                    sb.append(']');
                    report.append(" " + sb.toString());
                }
            }
            report.append('\n');
        }
        if (missing > 0) {
            if (!showFiles) {
                System.out.println("\nMISSING " + missing + " blocks of total size " + missize + " B");
            }
            res.corruptFiles++;
            switch (fixing) {
                case FIXING_NONE:
                    System.err.println("\n - ignoring corrupted " + file.getPath());
                    break;
                case FIXING_MOVE:
                    System.err.println("\n - moving to /lost+found: " + file.getPath());
                    lostFoundMove(file, blocks);
                    break;
                case FIXING_DELETE:
                    System.err.println("\n - deleting corrupted " + file.getPath());
                    dfs.delete(new UTF8(file.getPath()));
                    break;
                default:
                    break;
            }
        }
        if (showFiles) {
            if (missing > 0) {
                System.out.println(" MISSING " + missing + " blocks of total size " + missize + " B");
            } else {
                System.out.println(" OK");
            }
            if (showBlocks) {
                System.out.println(report.toString());
            }
        }
    }

    private void lostFoundMove(DFSFileInfo file, LocatedBlock[] blocks) {
        if (!lfInited) {
            lostFoundInit();
        }
        if (!lfInitedOk) {
            return;
        }
        UTF8 target = new UTF8(lostFound.toString() + file.getPath());
        String errmsg = "Failed to move " + file.getPath() + " to /lost+found";
        try {
            if (!dfs.mkdirs(target)) {
                System.err.println(errmsg);
                return;
            }
            // create chains
            int chain = 0;
            FSOutputStream fos = null;
            for (int i = 0; i < blocks.length; i++) {
                LocatedBlock lblock = blocks[i];
                DatanodeInfo[] locs = lblock.getLocations();
                if (locs == null || locs.length == 0) {
                    if (fos != null) {
                        fos.flush();
                        fos.close();
                        fos = null;
                    }
                    continue;
                }
                if (fos == null) {
                    fos = dfs.create(new UTF8(target.toString() + "/" + chain), true);
                    if (fos != null) {
                        chain++;
                    }
                }
                if (fos == null) {
                    System.err.println(errmsg + ": could not store chain " + chain);
                    // perhaps we should bail out here...
                    // return;
                    continue;
                }

                // copy the block. It's a pity it's not abstracted from DFSInputStream ...
                try {
                    copyBlock(lblock, fos);
                } catch (Exception e) {
                    e.printStackTrace();
                    // something went wrong copying this block...
                    System.err.println(" - could not copy block " + lblock.getBlock().getBlockName() + " to " + target);
                    fos.flush();
                    fos.close();
                    fos = null;
                }
            }
            if (fos != null) {
                fos.close();
            }
            System.err.println("\n - moved corrupted file " + file.getPath() + " to /lost+found");
            dfs.delete(new UTF8(file.getPath()));
        } catch (Exception e) {
            e.printStackTrace();
            System.err.println(errmsg + ": " + e.getMessage());
        }
    }

    /**
     * XXX (ab)这个方法的大部分是从{@link DFSClient}中逐字复制的，这很糟糕。<br/>
     * 这两个地方都应该进行重构，以提供复制块的方法。
     * @param lblock
     * @param fos
     * @throws Exception
     */
    private void copyBlock(LocatedBlock lblock, FSOutputStream fos) throws Exception {
        int failures = 0;
        InetSocketAddress targetAddr = null;
        TreeSet<DatanodeInfo> deadNodes = new TreeSet<DatanodeInfo>();
        Socket s = null;
        DataInputStream in = null;
        DataOutputStream out = null;
        while (s == null) {
            DatanodeInfo chosenNode;
            try {
                chosenNode = bestNode(lblock.getLocations(), deadNodes);
                targetAddr = DataNode.createSocketAddr(chosenNode.getName().toString());
            } catch (IOException ie) {
                if (failures >= DFSClient.MAX_BLOCK_ACQUIRE_FAILURES) {
                    throw new IOException("Could not obtain block " + lblock);
                }
                LOGGER.info("Could not obtain block from any node:  " + ie);
                try {
                    Thread.sleep(10000);
                } catch (InterruptedException iex) {
                }
                deadNodes.clear();
                failures++;
                continue;
            }
            try {
                s = new Socket();
                s.connect(targetAddr, FSConstants.READ_TIMEOUT);
                s.setSoTimeout(FSConstants.READ_TIMEOUT);
                // Xmit头信息到datanode
                out = new DataOutputStream(new BufferedOutputStream(s.getOutputStream()));
                out.write(FSConstants.OP_READSKIP_BLOCK);
                lblock.getBlock().write(out);
                out.writeLong(0L);
                out.flush();
                // 获取块中的字节，设置流
                in = new DataInputStream(new BufferedInputStream(s.getInputStream()));
                long curBlockSize = in.readLong();
                long amtSkipped = in.readLong();
                if (curBlockSize != lblock.getBlock().len) {
                    throw new IOException("Recorded block size is " + lblock.getBlock().len + ", but datanode reports size of " + curBlockSize);
                }
                if (amtSkipped != 0L) {
                    throw new IOException("Asked for offset of " + 0L + ", but only received offset of " + amtSkipped);
                }
            } catch (IOException ex) {
                // 将选择的节点放入死列表中，继续
                LOGGER.error("Failed to connect to " + targetAddr + ":" + ex);
                deadNodes.add(chosenNode);
                if (s != null) {
                    try {
                        s.close();
                    } catch (IOException iex) {
                    }
                }
                s = null;
            }
        }
        if (in == null) {
            throw new Exception("Could not open data stream for " + lblock.getBlock().getBlockName());
        }
        byte[] buf = new byte[1024];
        int cnt = 0;
        boolean success = true;
        try {
            while ((cnt = in.read(buf)) > 0) {
                fos.write(buf, 0, cnt);
            }
        } catch (Exception e) {
            e.printStackTrace();
            success = false;
        } finally {
            try {
                in.close();
            } catch (Exception e1) {
            }
            try {
                out.close();
            } catch (Exception e1) {
            }
            try {
                s.close();
            } catch (Exception e1) {
            }
        }
        if (!success) {
            throw new Exception("Could not copy block data for " + lblock.getBlock().getBlockName());
        }
    }

    /*
     * XXX (ab) See comment above for copyBlock().
     */
    Random r = new Random();

    /**
     * 选择数据流的最佳节点。<br/>
     * 如果可以的话，这是本地的。
     * @param nodes
     * @param deadNodes
     * @return
     * @throws IOException
     */
    private DatanodeInfo bestNode(DatanodeInfo[] nodes, TreeSet<DatanodeInfo> deadNodes) throws IOException {
        if ((nodes == null) ||
                (nodes.length - deadNodes.size() < 1)) {
            throw new IOException("No live nodes contain current block");
        }
        DatanodeInfo chosenNode = null;
        for (int i = 0; i < nodes.length; i++) {
            if (deadNodes.contains(nodes[i])) {
                continue;
            }
            String nodename = nodes[i].getName().toString();
            int colon = nodename.indexOf(':');
            if (colon >= 0) {
                nodename = nodename.substring(0, colon);
            }
            if (dfs.localName.equals(nodename)) {
                chosenNode = nodes[i];
                break;
            }
        }
        if (chosenNode == null) {
            do {
                chosenNode = nodes[Math.abs(r.nextInt()) % nodes.length];
            } while (deadNodes.contains(chosenNode));
        }
        return chosenNode;
    }

    /**
     * 初始化/lost+found目录
     */
    private void lostFoundInit() {
        lfInited = true;
        try {
            UTF8 lfName = new UTF8("/lost+found");
            // check that /lost+found exists
            if (!dfs.exists(lfName)) {
                lfInitedOk = dfs.mkdirs(lfName);
                lostFound = lfName;
            } else if (!dfs.isDirectory(lfName)) {
                System.err.println("Cannot use /lost+found : a regular file with this name exists.");
                lfInitedOk = false;
            } else { // exists and isDirectory
                lostFound = lfName;
                lfInitedOk = true;
            }
        } catch (Exception e) {
            e.printStackTrace();
            lfInitedOk = false;
        }
        if (lostFound == null) {
            System.err.println("Cannot initialize /lost+found .");
            lfInitedOk = false;
        }
    }

    /**
     * @param args
     */
    public static void main(String[] args) throws Exception {
        if (args.length == 0) {
            System.err.println("Usage: DFSck <path> [-move | -delete] [-files] [-blocks [-locations]]");
            System.err.println("\t<path>\tstart checking from this path");
            System.err.println("\t-move\tmove corrupted files to /lost+found");
            System.err.println("\t-delete\tdelete corrupted files");
            System.err.println("\t-files\tprint out files being checked");
            System.err.println("\t-blocks\tprint out block report");
            System.err.println("\t-locations\tprint out locations for every block");
            return;
        }
        Configuration conf = new Configuration();
        String path = args[0];
        boolean showFiles = false;
        boolean showBlocks = false;
        boolean showLocations = false;
        int fixing = FIXING_NONE;
        for (int i = 1; i < args.length; i++) {
            if ("-files".equals(args[i])) {
                showFiles = true;
            }
            if ("-blocks".equals(args[i])) {
                showBlocks = true;
            }
            if ("-locations".equals(args[i])) {
                showLocations = true;
            }
            if ("-move".equals(args[i])) {
                fixing = FIXING_MOVE;
            }
            if ("-delete".equals(args[i])) {
                fixing = FIXING_DELETE;
            }
        }
        DFSck fsck = new DFSck(conf, fixing, showFiles, showBlocks, showLocations);
        Result res = fsck.fsck(path);
        System.out.println();
        System.out.println(res);
        if (res.isHealthy()) {
            System.out.println("\n\nThe filesystem under path '" + args[0] + "' is HEALTHY");
        } else {
            System.out.println("\n\nThe filesystem under path '" + args[0] + "' is CORRUPT");
        }
    }

    /**
     * 检查结果，加上总体DFS统计数据。
     */
    public static class Result {
        private ArrayList missingIds = new ArrayList();
        private long missingSize = 0L;
        private long corruptFiles = 0L;
        private long overReplicatedBlocks = 0L;
        private long underReplicatedBlocks = 0L;
        private int replication = 0;
        private long totalBlocks = 0L;
        private long totalFiles = 0L;
        private long totalDirs = 0L;
        private long totalSize = 0L;

        /**
         * 如果没有丢失块，DFS被认为是健康的。
         * @return
         */
        public boolean isHealthy() {
            return missingIds.size() == 0;
        }

        /**
         * 添加一个缺失的块名，加上它的大小。
         */
        public void addMissing(String id, long size) {
            missingIds.add(id);
            missingSize += size;
        }

        /**
         * 返回一个缺少块名的列表(作为字符串列表)。
         */
        public ArrayList getMissingIds() {
            return missingIds;
        }

        /**
         * 返回丢失数据的总大小，单位为字节。
         */
        public long getMissingSize() {
            return missingSize;
        }

        public void setMissingSize(long missingSize) {
            this.missingSize = missingSize;
        }

        /**
         * 返回过度复制的块的数量。
         */
        public long getOverReplicatedBlocks() {
            return overReplicatedBlocks;
        }

        public void setOverReplicatedBlocks(long overReplicatedBlocks) {
            this.overReplicatedBlocks = overReplicatedBlocks;
        }

        /**
         * 返回实际的复制因子。
         */
        public float getReplicationFactor() {
            return (float) (totalBlocks * replication + overReplicatedBlocks - underReplicatedBlocks) / (float) totalBlocks;
        }

        /**
         * 返回未复制块的数量。注意:丢失的块不计算在这里。
         */
        public long getUnderReplicatedBlocks() {
            return underReplicatedBlocks;
        }

        public void setUnderReplicatedBlocks(long underReplicatedBlocks) {
            this.underReplicatedBlocks = underReplicatedBlocks;
        }

        /**
         * 返回此扫描期间遇到的目录总数。
         */
        public long getTotalDirs() {
            return totalDirs;
        }

        public void setTotalDirs(long totalDirs) {
            this.totalDirs = totalDirs;
        }

        /**
         * 返回此扫描期间遇到的文件总数。
         */
        public long getTotalFiles() {
            return totalFiles;
        }

        public void setTotalFiles(long totalFiles) {
            this.totalFiles = totalFiles;
        }

        /**
         * 返回扫描数据的总大小，单位为字节。
         */
        public long getTotalSize() {
            return totalSize;
        }

        public void setTotalSize(long totalSize) {
            this.totalSize = totalSize;
        }

        /**
         * 返回预期的复制因子，根据该因子计算复制过多或过少的块。<br/>
         * 注意:这个值来自为该工具提供的当前配置，因此它可能与DFS配置中的值不同。
         */
        public int getReplication() {
            return replication;
        }

        public void setReplication(int replication) {
            this.replication = replication;
        }

        /**
         * 返回扫描区域中的块总数。
         */
        public long getTotalBlocks() {
            return totalBlocks;
        }

        public void setTotalBlocks(long totalBlocks) {
            this.totalBlocks = totalBlocks;
        }

        @Override
        public String toString() {
            StringBuffer res = new StringBuffer();
            res.append("Status: " + (isHealthy() ? "HEALTHY" : "CORRUPT"));
            res.append("\n Total size:\t" + totalSize + " B");
            res.append("\n Total blocks:\t" + totalBlocks + " (avg. block size "
                    + (totalSize / totalBlocks) + " B)");
            res.append("\n Total dirs:\t" + totalDirs);
            res.append("\n Total files:\t" + totalFiles);
            if (missingSize > 0) {
                res.append("\n  ********************************");
                res.append("\n  CORRUPT FILES:\t" + corruptFiles);
                res.append("\n  MISSING BLOCKS:\t" + missingIds.size());
                res.append("\n  MISSING SIZE:\t\t" + missingSize + " B");
                res.append("\n  ********************************");
            }
            res.append("\n Over-replicated blocks:\t" + overReplicatedBlocks
                    + " (" + ((float) (overReplicatedBlocks * 100) / (float) totalBlocks)
                    + " %)");
            res.append("\n Under-replicated blocks:\t" + underReplicatedBlocks
                    + " (" + ((float) (underReplicatedBlocks * 100) / (float) totalBlocks)
                    + " %)");
            res.append("\n Target replication factor:\t" + replication);
            res.append("\n Real replication factor:\t" + getReplicationFactor());
            return res.toString();
        }

        /**
         * 返回断开的文件的数量。
         */
        public long getCorruptFiles() {
            return corruptFiles;
        }

        public void setCorruptFiles(long corruptFiles) {
            this.corruptFiles = corruptFiles;
        }
    }

}
