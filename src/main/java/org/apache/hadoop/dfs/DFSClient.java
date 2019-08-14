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
import org.apache.hadoop.fs.FSInputStream;
import org.apache.hadoop.fs.FSOutputStream;
import org.apache.hadoop.io.UTF8;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.util.Constants;
import org.apache.hadoop.util.Daemon;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.Random;
import java.util.TreeSet;
import java.util.Vector;

/**
 * DFSClient可以连接到Hadoop文件系统并执行基本的文件任务。<br/>
 * 它使用ClientProtocol与NameNode守护进程通信，并直接连接到datanode来读取/写入数据块。<br/>
 * Hadoop DFS用户应该获得DistributedFileSystem的一个实例，它使用DFSClient来处理文件系统任务。
 * @author 章云
 * @date 2019/8/9 9:13
 */
public class DFSClient implements FSConstants {
    private static final Logger LOGGER = LoggerFactory.getLogger(DFSClient.class);
    static int MAX_BLOCK_ACQUIRE_FAILURES = 3;
    ClientProtocol namenode;
    String localName;
    boolean running = true;
    Random r = new Random();
    String clientName;
    Daemon leaseChecker;
    private Configuration conf;

    /**
     * 创建一个连接到给定namenode服务器的新DFSClient。
     */
    public DFSClient(InetSocketAddress nameNodeAddr, Configuration conf) {
        this.conf = conf;
        this.namenode = RPC.getProxy(ClientProtocol.class, nameNodeAddr, conf);
        try {
            this.localName = InetAddress.getLocalHost().getHostName();
        } catch (UnknownHostException uhe) {
            this.localName = "";
        }
        this.clientName = "DFSClient_" + r.nextInt();
        this.leaseChecker = new Daemon(new LeaseChecker());
        this.leaseChecker.start();
    }

    public void close() throws IOException {
        this.running = false;
        try {
            leaseChecker.join();
        } catch (InterruptedException ie) {
        }
    }

    /**
     * 获取有关所指示块的位置的提示。<br/>
     * 返回的数组只要在指定的范围内有块即可。<br/>
     * 每个块可以有一个或多个位置。
     */
    public String[][] getHints(UTF8 src, long start, long len) throws IOException {
        return namenode.getHints(src.toString(), start, len);
    }

    /**
     * 创建一个输入流，它从namenode获取一个节点列表，然后从所有正确的位置读取。<br/>
     * 创建InputStream的内部子类，它执行正确的带外工作。
     */
    public FSInputStream open(UTF8 src) throws IOException {
        // 从namenode获取块信息
        return new DFSInputStream(src.toString());
    }

    public FSOutputStream create(UTF8 src, boolean overwrite) throws IOException {
        return new DFSOutputStream(src, overwrite);
    }

    /**
     * 直接连接到namenode并在那里操作结构。
     */
    public boolean rename(UTF8 src, UTF8 dst) throws IOException {
        return namenode.rename(src.toString(), dst.toString());
    }

    public boolean delete(UTF8 src) throws IOException {
        return namenode.delete(src.toString());
    }

    public boolean exists(UTF8 src) throws IOException {
        return namenode.exists(src.toString());
    }

    public boolean isDirectory(UTF8 src) throws IOException {
        return namenode.isDir(src.toString());
    }

    public DFSFileInfo[] listFiles(UTF8 src) throws IOException {
        return namenode.getListing(src.toString());
    }

    public long totalRawCapacity() throws IOException {
        long[] rawNums = namenode.getStats();
        return rawNums[0];
    }

    public long totalRawUsed() throws IOException {
        long[] rawNums = namenode.getStats();
        return rawNums[1];
    }

    public DatanodeInfo[] datanodeReport() throws IOException {
        return namenode.getDatanodeReport();
    }

    public boolean mkdirs(UTF8 src) throws IOException {
        return namenode.mkdirs(src.toString());
    }

    public void lock(UTF8 src, boolean exclusive) throws IOException {
        long start = System.currentTimeMillis();
        boolean hasLock = false;
        while (!hasLock) {
            hasLock = namenode.obtainLock(src.toString(), clientName, exclusive);
            if (!hasLock) {
                try {
                    Thread.sleep(400);
                    if (System.currentTimeMillis() - start > 5000) {
                        LOGGER.info("Waiting to retry lock for " + (System.currentTimeMillis() - start) + " ms.");
                        Thread.sleep(2000);
                    }
                } catch (InterruptedException ie) {
                }
            }
        }
    }

    public void release(UTF8 src) throws IOException {
        boolean hasReleased = false;
        while (!hasReleased) {
            hasReleased = namenode.releaseLock(src.toString(), clientName);
            if (!hasReleased) {
                LOGGER.info("Could not release.  Retrying...");
                try {
                    Thread.sleep(2000);
                } catch (InterruptedException ie) {
                }
            }
        }
    }

    /**
     * 选择数据流的最佳节点。<br/>
     * 如果可以的话，这是本地的。
     */
    private DatanodeInfo bestNode(DatanodeInfo[] nodes, TreeSet deadNodes) throws IOException {
        if ((nodes == null) || (nodes.length - deadNodes.size() < 1)) {
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
            if (localName.equals(nodename)) {
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

    /***************************************************************
     * 定期检查namenode，并在租期过半时续订所有租约。
     ***************************************************************/
    public class LeaseChecker implements Runnable {
        @Override
        public void run() {
            long lastRenewed = 0;
            while (running) {
                if (System.currentTimeMillis() - lastRenewed > (LEASE_PERIOD / 2)) {
                    try {
                        namenode.renewLease(clientName);
                        lastRenewed = System.currentTimeMillis();
                    } catch (IOException ie) {
                    }
                }
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException ie) {
                }
            }
        }
    }

    /****************************************************************
     * DFSInputStream提供来自命名文件的字节。<br/>
     * 它根据需要处理namenode和各种数据节点的协商。
     ****************************************************************/
    public class DFSInputStream extends FSInputStream {
        private Socket s = null;
        boolean closed = false;

        private String src;
        private DataInputStream blockStream;
        private Block[] blocks = null;
        private DatanodeInfo[][] nodes = null;
        private long pos = 0;
        private long filelen = 0;
        private long blockEnd = -1;

        public DFSInputStream(String src) throws IOException {
            this.src = src;
            openInfo();
            this.blockStream = null;
            for (int i = 0; i < blocks.length; i++) {
                this.filelen += blocks[i].getNumBytes();
            }
        }

        /**
         * 从namenode获取打开的文件信息
         */
        void openInfo() throws IOException {
            Block[] oldBlocks = this.blocks;
            LocatedBlock[] results = namenode.open(src);
            Vector<Block> blockV = new Vector<Block>();
            Vector<DatanodeInfo[]> nodeV = new Vector<DatanodeInfo[]>();
            for (int i = 0; i < results.length; i++) {
                blockV.add(results[i].getBlock());
                nodeV.add(results[i].getLocations());
            }
            Block[] newBlocks = blockV.toArray(new Block[blockV.size()]);
            if (oldBlocks != null) {
                for (int i = 0; i < oldBlocks.length; i++) {
                    if (!oldBlocks[i].equals(newBlocks[i])) {
                        throw new IOException("Blocklist for " + src + " has changed!");
                    }
                }
                if (oldBlocks.length != newBlocks.length) {
                    throw new IOException("Blocklist for " + src + " now has different length");
                }
            }
            this.blocks = newBlocks;
            this.nodes = nodeV.toArray(new DatanodeInfo[nodeV.size()][]);
        }

        /**
         * 打开DataInputStream到DataNode，以便从DataNode读取数据。<br/>
         * 我们从namenode获取块ID和启动时目标的ID。
         */
        private synchronized void blockSeekTo(long target) throws IOException {
            if (target >= filelen) {
                throw new IOException("Attempted to read past end of file");
            }
            if (s != null) {
                s.close();
                s = null;
            }
            // 计算所需的块
            int targetBlock = -1;
            long targetBlockStart = 0;
            long targetBlockEnd = 0;
            for (int i = 0; i < blocks.length; i++) {
                long blocklen = blocks[i].getNumBytes();
                targetBlockEnd = targetBlockStart + blocklen - 1;
                if (target >= targetBlockStart && target <= targetBlockEnd) {
                    targetBlock = i;
                    break;
                } else {
                    targetBlockStart = targetBlockEnd + 1;
                }
            }
            if (targetBlock < 0) {
                throw new IOException("Impossible situation: could not find target position " + target);
            }
            long offsetIntoBlock = target - targetBlockStart;
            // 连接到所需块的最佳DataNode，并带有潜在的偏移量
            int failures = 0;
            InetSocketAddress targetAddr = null;
            TreeSet<DatanodeInfo> deadNodes = new TreeSet<DatanodeInfo>();
            while (s == null) {
                DatanodeInfo chosenNode;
                try {
                    chosenNode = bestNode(nodes[targetBlock], deadNodes);
                    targetAddr = DataNode.createSocketAddr(chosenNode.getName().toString());
                } catch (IOException ie) {
                    String blockInfo = blocks[targetBlock] + " file=" + src + " offset=" + target;
                    if (failures >= MAX_BLOCK_ACQUIRE_FAILURES) {
                        throw new IOException("Could not obtain block: " + blockInfo);
                    }
                    if (nodes[targetBlock] == null || nodes[targetBlock].length == 0) {
                        LOGGER.info("No node available for block: " + blockInfo);
                    }
                    LOGGER.info("Could not obtain block from any node:  " + ie);
                    try {
                        Thread.sleep(3000);
                    } catch (InterruptedException iex) {
                    }
                    deadNodes.clear();
                    openInfo();
                    failures++;
                    continue;
                }
                try {
                    s = new Socket();
                    s.connect(targetAddr, READ_TIMEOUT);
                    s.setSoTimeout(READ_TIMEOUT);
                    // Xmit头信息到datanode
                    DataOutputStream out = new DataOutputStream(new BufferedOutputStream(s.getOutputStream()));
                    out.write(OP_READSKIP_BLOCK);
                    blocks[targetBlock].write(out);
                    out.writeLong(offsetIntoBlock);
                    out.flush();
                    // 获取块中的字节，设置流
                    DataInputStream in = new DataInputStream(new BufferedInputStream(s.getInputStream()));
                    long curBlockSize = in.readLong();
                    long amtSkipped = in.readLong();
                    if (curBlockSize != blocks[targetBlock].getNumBytes()) {
                        throw new IOException("Recorded block size is " + blocks[targetBlock].getNumBytes() + ", but datanode reports size of " + curBlockSize);
                    }
                    if (amtSkipped != offsetIntoBlock) {
                        throw new IOException("Asked for offset of " + offsetIntoBlock + ", but only received offset of " + amtSkipped);
                    }

                    this.pos = target;
                    this.blockEnd = targetBlockEnd;
                    this.blockStream = in;
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
        }

        /**
         * 关闭它!
         */
        @Override
        public synchronized void close() throws IOException {
            if (closed) {
                throw new IOException("Stream closed");
            }

            if (s != null) {
                blockStream.close();
                s.close();
                s = null;
            }
            super.close();
            closed = true;
        }

        @Override
        public synchronized int read() throws IOException {
            if (closed) {
                throw new IOException("Stream closed");
            }
            int result = -1;
            if (pos < filelen) {
                if (pos > blockEnd) {
                    blockSeekTo(pos);
                }
                result = blockStream.read();
                if (result >= 0) {
                    pos++;
                }
            }
            return result;
        }

        /**
         * 读取整个缓冲区。
         */
        @Override
        public synchronized int read(byte[] buf, int off, int len) throws IOException {
            if (closed) {
                throw new IOException("Stream closed");
            }
            if (pos < filelen) {
                if (pos > blockEnd) {
                    blockSeekTo(pos);
                }
                int result = blockStream.read(buf, off, Math.min(len, (int) (blockEnd - pos + 1)));
                if (result >= 0) {
                    pos += result;
                }
                return result;
            }
            return -1;
        }

        /**
         * 寻找一个新的任意位置
         */
        @Override
        public synchronized void seek(long targetPos) throws IOException {
            if (targetPos >= filelen) {
                throw new IOException("Cannot seek after EOF");
            }
            pos = targetPos;
            blockEnd = -1;
        }

        @Override
        public synchronized long getPos() throws IOException {
            return pos;
        }

        @Override
        public synchronized int available() throws IOException {
            if (closed) {
                throw new IOException("Stream closed");
            }
            return (int) (filelen - pos);
        }

        /**
         * 我们绝对不支持标记
         */
        @Override
        public boolean markSupported() {
            return false;
        }

        @Override
        public void mark(int readLimit) {
        }

        @Override
        public void reset() throws IOException {
            throw new IOException("Mark not supported");
        }
    }

    /****************************************************************
     * DFSOutputStream从一个字节流创建文件。
     ****************************************************************/
    public class DFSOutputStream extends FSOutputStream {
        private Socket s;
        boolean closed = false;

        private byte[] outBuf = new byte[BUFFER_SIZE];
        private int pos = 0;

        private UTF8 src;
        private boolean overwrite;
        private boolean firstTime = true;
        private DataOutputStream blockStream;
        private DataInputStream blockReplyStream;
        private File backupFile;
        private OutputStream backupStream;
        private Block block;
        private long filePos = 0;
        private int bytesWrittenToBlock = 0;

        /**
         * 为给定的DataNode创建一个新的输出流。
         */
        public DFSOutputStream(UTF8 src, boolean overwrite) throws IOException {
            this.src = src;
            this.overwrite = overwrite;
            this.backupFile = newBackupFile();
            this.backupStream = new FileOutputStream(backupFile);
        }

        private File newBackupFile() throws IOException {
            File result = conf.getFile("dfs.data.dir", "tmp" + File.separator + "client-" + Math.abs(r.nextLong()));
            result.deleteOnExit();
            return result;
        }

        /**
         * 打开DataOutputStream到DataNode，以便将其写入。<br/>
         * 这发生在创建文件时，每次分配一个新块时。<br/>
         * 必须从namenode获取块ID和目标的ID。
         */
        private synchronized void nextBlockOutputStream() throws IOException {
            boolean retry;
            long start = System.currentTimeMillis();
            do {
                retry = false;
                long localstart = System.currentTimeMillis();
                boolean blockComplete = false;
                LocatedBlock lb = null;
                while (!blockComplete) {
                    if (firstTime) {
                        lb = namenode.create(src.toString(), clientName, localName, overwrite);
                    } else {
                        lb = namenode.addBlock(src.toString(), localName);
                    }
                    if (lb == null) {
                        try {
                            Thread.sleep(400);
                            if (System.currentTimeMillis() - localstart > 5000) {
                                LOGGER.info("Waiting to find new output block node for " + (System.currentTimeMillis() - start) + "ms");
                            }
                        } catch (InterruptedException ie) {
                        }
                    } else {
                        blockComplete = true;
                    }
                }

                block = lb.getBlock();
                DatanodeInfo[] nodes = lb.getLocations();
                // 连接到列表中的第一个DataNode。如果失败，则中止。
                InetSocketAddress target = DataNode.createSocketAddr(nodes[0].getName().toString());
                try {
                    s = new Socket();
                    s.connect(target, READ_TIMEOUT);
                    s.setSoTimeout(READ_TIMEOUT);
                } catch (IOException ie) {
                    // 连接失败。让我们等一会儿再试
                    try {
                        if (System.currentTimeMillis() - start > 5000) {
                            LOGGER.info("Waiting to find target node: " + target);
                        }
                        Thread.sleep(6000);
                    } catch (InterruptedException iex) {
                    }
                    if (firstTime) {
                        namenode.abandonFileInProgress(src.toString());
                    } else {
                        namenode.abandonBlock(block, src.toString());
                    }
                    retry = true;
                    continue;
                }
                // Xmit头信息到datanode
                DataOutputStream out = new DataOutputStream(new BufferedOutputStream(s.getOutputStream()));
                out.write(OP_WRITE_BLOCK);
                out.writeBoolean(false);
                block.write(out);
                out.writeInt(nodes.length);
                for (int i = 0; i < nodes.length; i++) {
                    nodes[i].write(out);
                }
                out.write(CHUNKED_ENCODING);
                bytesWrittenToBlock = 0;
                blockStream = out;
                blockReplyStream = new DataInputStream(new BufferedInputStream(s.getInputStream()));
            } while (retry);
            firstTime = false;
        }

        /**
         * 这里我们指的是文件pos
         */
        @Override
        public synchronized long getPos() {
            return filePos;
        }

        /**
         * 将指定的字节写入此输出流。
         */
        @Override
        public synchronized void write(int b) throws IOException {
            if (closed) {
                throw new IOException("Stream closed");
            }

            if ((bytesWrittenToBlock + pos == BLOCK_SIZE) ||
                    (pos >= BUFFER_SIZE)) {
                flush();
            }
            outBuf[pos++] = (byte) b;
            filePos++;
        }

        /**
         * 将指定的字节写入此输出流。
         */
        @Override
        public synchronized void write(byte[] b, int off, int len) throws IOException {
            if (closed) {
                throw new IOException("Stream closed");
            }
            while (len > 0) {
                int remaining = BUFFER_SIZE - pos;
                int toWrite = Math.min(remaining, len);
                System.arraycopy(b, off, outBuf, pos, toWrite);
                pos += toWrite;
                off += toWrite;
                len -= toWrite;
                filePos += toWrite;

                if ((bytesWrittenToBlock + pos >= BLOCK_SIZE) || (pos == BUFFER_SIZE)) {
                    flush();
                }
            }
        }

        /**
         * 刷新缓冲区，如有必要，将流获取到新块。
         */
        @Override
        public synchronized void flush() throws IOException {
            if (closed) {
                throw new IOException("Stream closed");
            }

            if (bytesWrittenToBlock + pos >= BLOCK_SIZE) {
                flushData(BLOCK_SIZE - bytesWrittenToBlock);
            }
            if (bytesWrittenToBlock == BLOCK_SIZE) {
                endBlock();
            }
            flushData(pos);
        }

        /**
         * 实际将累积的字节刷新到远程节点，但不会超过指定的字节数。
         */
        private synchronized void flushData(int maxPos) throws IOException {
            int workingPos = Math.min(pos, maxPos);
            if (workingPos > 0) {
                // 对于本地块备份，只写字节
                backupStream.write(outBuf, 0, workingPos);
                // 跟踪位置
                bytesWrittenToBlock += workingPos;
                System.arraycopy(outBuf, workingPos, outBuf, 0, pos - workingPos);
                pos -= workingPos;
            }
        }

        /**
         * 我们已经完成了对当前块的写入。
         */
        private synchronized void endBlock() throws IOException {
            // 使用本地副本
            backupStream.close();
            // 将其发送到datanode
            boolean mustRecover = true;
            while (mustRecover) {
                nextBlockOutputStream();
                InputStream in = new FileInputStream(backupFile);
                try {
                    byte[] buf = new byte[BUFFER_SIZE];
                    int bytesRead = in.read(buf);
                    while (bytesRead > 0) {
                        blockStream.writeLong((long) bytesRead);
                        blockStream.write(buf, 0, bytesRead);
                        bytesRead = in.read(buf);
                    }
                    internalClose();
                    mustRecover = false;
                } catch (IOException ie) {
                    handleSocketException(ie);
                } finally {
                    in.close();
                }
            }
            // 删除本地备份，启动新的备份
            backupFile.delete();
            backupFile = newBackupFile();
            backupStream = new FileOutputStream(backupFile);
            bytesWrittenToBlock = 0;
        }

        /**
         * 关闭下游到远程datanode。
         */
        private synchronized void internalClose() throws IOException {
            blockStream.writeLong(0);
            blockStream.flush();
            long complete = blockReplyStream.readLong();
            if (complete != WRITE_COMPLETE) {
                LOGGER.info("Did not receive WRITE_COMPLETE flag: " + complete);
                throw new IOException("Did not receive WRITE_COMPLETE_FLAG: " + complete);
            }
            LocatedBlock lb = new LocatedBlock();
            lb.readFields(blockReplyStream);
            namenode.reportWrittenBlock(lb);
            s.close();
            s = null;
        }

        private void handleSocketException(IOException ie) throws IOException {
            LOGGER.warn("Error while writing.", ie);
            try {
                if (s != null) {
                    s.close();
                    s = null;
                }
            } catch (IOException ie2) {
                LOGGER.warn("Error closing socket.", ie2);
            }
            namenode.abandonBlock(block, src.toString());
        }

        /**
         * 关闭此输出流并释放与此流关联的任何系统资源。
         */
        @Override
        public synchronized void close() throws IOException {
            if (closed) {
                throw new IOException("Stream closed");
            }
            flush();
            if (filePos == 0 || bytesWrittenToBlock != 0) {
                try {
                    endBlock();
                } catch (IOException e) {
                    namenode.abandonFileInProgress(src.toString());
                    throw e;
                }
            }
            backupStream.close();
            backupFile.delete();
            if (s != null) {
                s.close();
                s = null;
            }
            super.close();
            long localstart = System.currentTimeMillis();
            boolean fileComplete = false;
            while (!fileComplete) {
                fileComplete = namenode.complete(src.toString(), clientName.toString());
                if (!fileComplete) {
                    try {
                        Thread.sleep(400);
                        if (System.currentTimeMillis() - localstart > 5000) {
                            LOGGER.info("Could not complete file, retrying...");
                        }
                    } catch (InterruptedException ie) {
                    }
                }
            }
            closed = true;
        }
    }
}
