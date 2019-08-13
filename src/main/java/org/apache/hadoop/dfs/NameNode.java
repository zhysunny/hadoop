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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.UTF8;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.ipc.Server;
import org.apache.hadoop.util.Constants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;

/**
 * NameNode同时充当Hadoop DFS的目录名称空间管理器和“inode表”。<br/>
 * 在任何DFS部署中都运行一个NameNode。<br/>
 * (除非有第二个备份/故障转移NameNode)。<br/>
 * NameNode控制两个关键表:<br/>
 * 1)文件名- > blocksequence(名称空间)<br/>
 * 2)块- > machinelist(“节点”)<br/>
 * 第一个表存储在磁盘上，非常宝贵。<br/>
 * 每当出现NameNode时，都会重新构建第二个表。<br/>
 * “NameNode”指的是这个类和“NameNode服务器”。<br/>
 * “fsnamessystem”类实际上执行大部分文件系统管理。<br/>
 * “NameNode”类本身的大部分内容是将IPC接口公开给外部世界，以及一些配置管理。<br/>
 * NameNode实现了ClientProtocol接口，该接口允许客户机请求DFS服务。<br/>
 * ClientProtocol不是为DFS客户机代码的作者直接使用而设计的。<br/>
 * 最终用户应该使用org.apache.nutch.hadoop.fs。文件系统类。<br/>
 * NameNode还实现了DatanodeProtocol接口，由实际存储DFS数据块的DataNode程序使用。<br/>
 * 这些方法由DFS部署中的所有数据节点反复自动调用。
 * @author 章云
 * @date 2019/8/12 11:18
 */
public class NameNode implements ClientProtocol, DatanodeProtocol, FSConstants {
    private static final Logger LOGGER = LoggerFactory.getLogger(NameNode.class);

    private FSNamesystem namesystem;
    private Server server;
    private int handlerCount;

    /**
     * 仅用于测试目的
     */
    private boolean stopRequested = false;

    /**
     * 在默认位置创建一个NameNode
     * @param conf
     * @throws IOException
     */
    public NameNode(Configuration conf) throws IOException {
        this(getDir(conf), DataNode.createSocketAddr(Constants.FS_DEFAULT_NAME).getPort(), conf);
    }

    /**
     * 在指定的位置创建一个NameNode并启动它。
     * @param dir  namenode存储目录
     * @param port namenode端口
     * @param conf 配置类
     * @throws IOException
     */
    public NameNode(File dir, int port, Configuration conf) throws IOException {
        this.namesystem = new FSNamesystem(dir, conf);
        this.handlerCount = Constants.DFS_NAMENODE_HANDLER_COUNT;
        this.server = RPC.getServer(this, port, handlerCount, false, conf);
        this.server.start();
    }

    /**
     * namenode数据存储目录
     * @param conf
     * @return
     */
    private static File getDir(Configuration conf) {
        return new File(Constants.DFS_NAME_DIR);
    }

    /**
     * 等待服务完成。<br/>
     * (正常情况下，它会一直运行下去。)
     */
    public void join() {
        try {
            this.server.join();
        } catch (InterruptedException ie) {
        }
    }

    /**
     * 停止所有NameNode线程，等待所有线程完成。<br/>
     * 只有包访问，因为这是用于JUnit测试的。
     */
    void stop() {
        if (!stopRequested) {
            stopRequested = true;
            namesystem.close();
            server.stop();
        }
    }

    /////////////////////////////////////////////////////
    // ClientProtocol
    /////////////////////////////////////////////////////

    @Override
    public LocatedBlock[] open(String src) throws IOException {
        Object[] openResults = namesystem.open(new UTF8(src));
        if (openResults == null) {
            throw new IOException("Cannot open filename " + src);
        } else {
            Block[] blocks = (Block[]) openResults[0];
            DatanodeInfo[][] sets = (DatanodeInfo[][]) openResults[1];
            LocatedBlock[] results = new LocatedBlock[blocks.length];
            for (int i = 0; i < blocks.length; i++) {
                results[i] = new LocatedBlock(blocks[i], sets[i]);
            }
            return results;
        }
    }

    @Override
    public LocatedBlock create(String src, String clientName, String clientMachine, boolean overwrite) throws IOException {
        Object[] results = namesystem.startFile(new UTF8(src), new UTF8(clientName), new UTF8(clientMachine), overwrite);
        if (results == null) {
            throw new IOException("Cannot create file " + src + " on client " + clientName);
        } else {
            Block b = (Block) results[0];
            DatanodeInfo[] targets = (DatanodeInfo[]) results[1];
            return new LocatedBlock(b, targets);
        }
    }

    @Override
    public LocatedBlock addBlock(String src, String clientMachine) throws IOException {
        int retries = 5;
        Object[] results = namesystem.getAdditionalBlock(new UTF8(src), new UTF8(clientMachine));
        while (results != null && results[0] == null && retries > 0) {
            try {
                Thread.sleep(100);
            } catch (InterruptedException ie) {
            }
            results = namesystem.getAdditionalBlock(new UTF8(src), new UTF8(clientMachine));
            retries--;
        }

        if (results == null) {
            throw new IOException("Cannot obtain additional block for file " + src);
        } else if (results[0] == null) {
            return null;
        } else {
            Block b = (Block) results[0];
            DatanodeInfo[] targets = (DatanodeInfo[]) results[1];
            return new LocatedBlock(b, targets);
        }
    }

    /**
     * 客户端可以在它所写的一组已写块中报告。<br/>
     * 这些块通过客户机而不是datanode报告，以防止奇怪的心跳争用条件。
     */
    @Override
    public void reportWrittenBlock(LocatedBlock lb) throws IOException {
        Block b = lb.getBlock();
        DatanodeInfo[] targets = lb.getLocations();
        for (int i = 0; i < targets.length; i++) {
            namesystem.blockReceived(b, targets[i].getName());
        }
    }

    /**
     * 客户需要放弃交易。
     */
    @Override
    public void abandonBlock(Block b, String src) throws IOException {
        if (!namesystem.abandonBlock(b, new UTF8(src))) {
            throw new IOException("Cannot abandon block during write to " + src);
        }
    }

    @Override
    public void abandonFileInProgress(String src) {
        namesystem.abandonFileInProgress(new UTF8(src));
    }

    @Override
    public boolean complete(String src, String clientName) throws IOException {
        int returnCode = namesystem.completeFile(new UTF8(src), new UTF8(clientName));
        if (returnCode == STILL_WAITING) {
            return false;
        } else if (returnCode == COMPLETE_SUCCESS) {
            return true;
        } else {
            throw new IOException("Could not complete write to file " + src + " by " + clientName);
        }
    }

    @Override
    public String[][] getHints(String src, long start, long len) {
        UTF8[][] hosts = namesystem.getDatanodeHints(new UTF8(src), start, len);
        if (hosts == null) {
            return new String[0][];
        } else {
            String[][] results = new String[hosts.length][];
            for (int i = 0; i < hosts.length; i++) {
                results[i] = new String[hosts[i].length];
                for (int j = 0; j < results[i].length; j++) {
                    results[i][j] = hosts[i][j].toString();
                }
            }
            return results;
        }
    }

    @Override
    public boolean rename(String src, String dst) {
        return namesystem.renameTo(new UTF8(src), new UTF8(dst));
    }

    @Override
    public boolean delete(String src) {
        return namesystem.delete(new UTF8(src));
    }

    @Override
    public boolean exists(String src) {
        return namesystem.exists(new UTF8(src));
    }

    @Override
    public boolean isDir(String src) {
        return namesystem.isDir(new UTF8(src));
    }

    @Override
    public boolean mkdirs(String src) {
        return namesystem.mkdirs(new UTF8(src));
    }

    @Override
    public boolean obtainLock(String src, String clientName, boolean exclusive) throws IOException {
        int returnCode = namesystem.obtainLock(new UTF8(src), new UTF8(clientName), exclusive);
        if (returnCode == COMPLETE_SUCCESS) {
            return true;
        } else if (returnCode == STILL_WAITING) {
            return false;
        } else {
            throw new IOException("Failure when trying to obtain lock on " + src);
        }
    }

    @Override
    public boolean releaseLock(String src, String clientName) throws IOException {
        int returnCode = namesystem.releaseLock(new UTF8(src), new UTF8(clientName));
        if (returnCode == COMPLETE_SUCCESS) {
            return true;
        } else if (returnCode == STILL_WAITING) {
            return false;
        } else {
            throw new IOException("Failure when trying to release lock on " + src);
        }
    }

    @Override
    public void renewLease(String clientName) {
        namesystem.renewLease(new UTF8(clientName));
    }

    @Override
    public DFSFileInfo[] getListing(String src) {
        return namesystem.getListing(new UTF8(src));
    }

    @Override
    public long[] getStats() {
        long[] results = new long[2];
        results[0] = namesystem.totalCapacity();
        results[1] = namesystem.totalCapacity() - namesystem.totalRemaining();
        return results;
    }

    @Override
    public DatanodeInfo[] getDatanodeReport() throws IOException {
        DatanodeInfo[] results = namesystem.datanodeReport();
        if (results == null || results.length == 0) {
            throw new IOException("Cannot find datanode report");
        }
        return results;
    }

    ////////////////////////////////////////////////////////////////
    // DatanodeProtocol
    ////////////////////////////////////////////////////////////////

    @Override
    public void sendHeartbeat(String sender, long capacity, long remaining) {
        namesystem.gotHeartbeat(new UTF8(sender), capacity, remaining);
    }

    @Override
    public Block[] blockReport(String sender, Block[] blocks) {
        LOGGER.info("Block report from " + sender + ": " + blocks.length + " blocks.");
        return namesystem.processReport(blocks, new UTF8(sender));
    }

    @Override
    public void blockReceived(String sender, Block[] blocks) {
        for (int i = 0; i < blocks.length; i++) {
            namesystem.blockReceived(blocks[i], new UTF8(sender));
        }
    }

    @Override
    public void errorReport(String sender, String msg) {
        // Log error message from datanode
        //LOG.info("Report from " + sender + ": " + msg);
    }

    /**
     * 返回一个面向块的命令，以便datanode执行。<br/>
     * 这将是一个转移或删除操作。
     */
    @Override
    public BlockCommand getBlockwork(String sender, int xmitsInProgress) {
        //
        // 请求执行挂起的传输(如果有的话)
        //
        Object[] xferResults = namesystem.pendingTransfers(new DatanodeInfo(new UTF8(sender)), xmitsInProgress);
        if (xferResults != null) {
            return new BlockCommand((Block[]) xferResults[0], (DatanodeInfo[][]) xferResults[1]);
        }

        //如果没有传输，检查最近删除的应该删除的块。
        //这不是一个全datanode扫描，就像在块报告中所做的那样。
        //这只是一个小的快速移动的块，刚刚被删除。
        Block[] blocks = namesystem.blocksToInvalidate(new UTF8(sender));
        if (blocks != null) {
            return new BlockCommand(blocks);
        }
        return null;
    }

    /**
     * namenode启动函数
     * @param argv
     * @throws Exception
     */
    public static void main(String[] argv) throws Exception {
        Configuration conf = new Configuration();

        if (argv.length == 1 && "-format".equals(argv[0])) {
            // -format格式化namenode磁盘
            File dir = getDir(conf);
            if (dir.exists()) {
                // 是否需要重新格式化
                System.err.print("Re-format filesystem in " + dir + " ? (Y or N) ");
                // 无法输入，有问题
                if (System.in.read() != 'Y') {
                    System.err.println("Format aborted.");
                    System.exit(1);
                }
            }
            // 格式化一个新的文件系统。销毁可能已经存在于此位置的任何文件系统。
            // 删除image和edits文件
            FSDirectory.format(dir, conf);
            System.out.println("Formatted：" + dir);
            System.exit(0);
        }
        NameNode namenode = new NameNode(conf);
        namenode.join();
    }
}
