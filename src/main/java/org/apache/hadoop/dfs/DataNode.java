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

import org.apache.hadoop.ipc.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.util.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.net.*;
import java.util.*;

/**
 * DataNode是一个类(和程序)，它为DFS部署存储一组块。
 * 一个部署可以有一个或多个datanode。
 * 每个DataNode定期与一个NameNode通信。
 * 它还不时地与客户机代码和其他数据节点通信。
 * datanode存储一系列命名块。
 * DataNode允许客户端代码读取这些块，或者编写新的块数据。
 * DataNode还可以响应来自其NameNode的指令，删除块或将块复制到/从其他DataNode。
 * DataNode只维护一个关键表:block->字节流(BLOCK_SIZE或更小)
 * 此信息存储在本地磁盘上。
 * DataNode在启动时以及之后经常向NameNode报告表的内容。
 * 数据节点将它们的生命花费在无休止的循环中，要求NameNode做一些事情。
 * NameNode不能直接连接到数据节点;NameNode只返回由DataNode调用的函数的值。
 * 数据节点维护一个开放的服务器套接字，以便客户机代码或其他数据节点能够读取/写入数据。
 * 此服务器的主机/端口报告给NameNode，然后NameNode将该信息发送给客户机或其他可能感兴趣的数据节点。
 * @author 章云
 * @date 2019/8/10 14:57
 */
public class DataNode implements FSConstants, Runnable {
    private static final Logger LOGGER = LoggerFactory.getLogger(DataNode.class);
    //
    // 提醒- mjc -我可能会带“maxgigs”回来，这样用户可以人为地限制空间修改翻译结果
    //private static final long GIGABYTE = 1024 * 1024 * 1024;
    //private static long numGigs = Configuration.get().getLong("dfs.datanode.maxgigs", 100);
    //

    /**
     * 方法从字符串构建socket地址
     * @param s 只能是host:port
     * @return
     * @throws IOException
     */
    public static InetSocketAddress createSocketAddr(String s) {
        String target = s;
        int colonIndex = target.indexOf(':');
        if (colonIndex < 0) {
            throw new RuntimeException("Not a host:port pair: " + s);
        }
        String host = target.substring(0, colonIndex);
        int port = Integer.parseInt(target.substring(colonIndex + 1));
        return new InetSocketAddress(host, port);
    }


    private static Vector subThreadList = null;
    DatanodeProtocol namenode;
    FSDataset data;
    String localName;
    boolean shouldRun = true;
    Vector<Block> receivedBlockList = new Vector<Block>();
    int xmitsInProgress = 0;
    Daemon dataXceiveServer = null;
    long blockReportInterval;
    private long datanodeStartupPeriod;
    private Configuration fConf;

    /**
     * 创建给定配置和dataDir的DataNode。
     * “dataDir”是存储块的地方。
     */
    public DataNode(Configuration conf, String datadir) throws IOException {
        this(InetAddress.getLocalHost().getHostName(),
                new File(datadir),
                createSocketAddr(Constants.FS_DEFAULT_NAME), conf);
    }

    /**
     * 还可以使用显式给定的配置信息创建DataNode。
     */
    public DataNode(String machineName, File datadir, InetSocketAddress nameNodeAddr, Configuration conf) throws IOException {
        this.namenode = RPC.getProxy(DatanodeProtocol.class, nameNodeAddr, conf);
        this.data = new FSDataset(datadir, conf);
        ServerSocket ss = null;
        int tmpPort = Constants.DFS_DATANODE_PORT;
        while (ss == null) {
            try {
                ss = new ServerSocket(tmpPort);
                LOGGER.info("Opened server at " + tmpPort);
            } catch (IOException ie) {
                LOGGER.info("Could not open server at " + tmpPort + ", trying new port");
                tmpPort++;
            }
        }
        this.localName = machineName + ":" + tmpPort;
        this.dataXceiveServer = new Daemon(new DataXceiveServer(ss));
        this.dataXceiveServer.start();
        long blockReportIntervalBasis = Constants.DFS_BLOCKREPORT_INTERVALMSEC;
        this.blockReportInterval = blockReportIntervalBasis - new Random().nextInt((int) (blockReportIntervalBasis / 10));
        this.datanodeStartupPeriod = Constants.DFS_DATANODE_STARTUPMSEC;
    }

    /**
     * 返回namenode的标识符
     */
    public String getNamenode() {
        return "<namenode>";
    }

    /**
     * 关闭datanode的这个实例。
     * 仅在关机完成后返回。
     */
    void shutdown() {
        this.shouldRun = false;
        ((DataXceiveServer) this.dataXceiveServer.getRunnable()).kill();
        try {
            this.dataXceiveServer.join();
        } catch (InterruptedException ie) {
        }
    }

    /**
     * DataNode的主回路。
     * 运行到关机，永远调用远程NameNode函数。
     */
    public void offerService() throws Exception {
        long wakeups = 0;
        long lastHeartbeat = 0, lastBlockReport = 0;
        long sendStart = System.currentTimeMillis();
        int heartbeatsSent = 0;
        LOGGER.info("using BLOCKREPORT_INTERVAL of " + blockReportInterval + "msec");

        // 现在循环很长时间…
        while (shouldRun) {
            long now = System.currentTimeMillis();

            // 每隔一段时间，发送心跳或阻塞报告
            synchronized (receivedBlockList) {
                if (now - lastHeartbeat > HEARTBEAT_INTERVAL) {
                    //
                    //所有心跳消息包括以下信息:
                    //——Datanode名称
                    //——数据传输端口
                    //——总容量
                    //——剩余字节
                    //
                    namenode.sendHeartbeat(localName, data.getCapacity(), data.getRemaining());
                    LOGGER.info("Just sent heartbeat, with name " + localName);
                    lastHeartbeat = now;
                }
                if (now - lastBlockReport > blockReportInterval) {
                    //
                    // 发送最新的区块信息报告，如果计时器已经过期。
                    // 获取一个本地块的列表，这些块已经过时，可以安全地GC'ed。
                    //
                    Block[] toDelete = namenode.blockReport(localName, data.getBlockReport());
                    data.invalidate(toDelete);
                    lastBlockReport = now;
                    continue;
                }
                if (receivedBlockList.size() > 0) {
                    //
                    // 将新接收到的blockids发送到namenode
                    //
                    Block[] blockArray = receivedBlockList.toArray(new Block[receivedBlockList.size()]);
                    receivedBlockList.removeAllElements();
                    namenode.blockReceived(localName, blockArray);
                }

                //
                //只在启动静默期之后执行块操作(传输、删除)。
                //假设所有的数据节点都将一起启动，但是namenode可能在此之前已经启动了一段时间。
                //(在网络中断的情况下尤其如此。)
                //因此，等待一段时间将连接时间传递到第一个块传输。
                //否则，我们会不必要地转移很多块。
                //
                if (now - sendStart > datanodeStartupPeriod) {
                    //
                    // Check to see if there are any block-instructions from the
                    // namenode that this datanode should perform.
                    //
                    BlockCommand cmd = namenode.getBlockwork(localName, xmitsInProgress);
                    if (cmd != null && cmd.transferBlocks()) {
                        //
                        // Send a copy of a block to another datanode
                        //
                        Block[] blocks = cmd.getBlocks();
                        DatanodeInfo[][] xferTargets = cmd.getTargets();

                        for (int i = 0; i < blocks.length; i++) {
                            if (!data.isValidBlock(blocks[i])) {
                                String errStr = "Can't send invalid block " + blocks[i];
                                LOGGER.info(errStr);
                                namenode.errorReport(localName, errStr);
                                break;
                            } else {
                                if (xferTargets[i].length > 0) {
                                    LOGGER.info("Starting thread to transfer block " + blocks[i] + " to " + xferTargets[i]);
                                    new Daemon(new DataTransfer(xferTargets[i], blocks[i])).start();
                                }
                            }
                        }
                    } else if (cmd != null && cmd.invalidateBlocks()) {
                        //
                        // Some local block(s) are obsolete and can be 
                        // safely garbage-collected.
                        //
                        data.invalidate(cmd.getBlocks());
                    }
                }

                //
                // There is no work to do;  sleep until hearbeat timer elapses, 
                // or work arrives, and then iterate again.
                //
                long waitTime = HEARTBEAT_INTERVAL - (now - lastHeartbeat);
                if (waitTime > 0 && receivedBlockList.size() == 0) {
                    try {
                        receivedBlockList.wait(waitTime);
                    } catch (InterruptedException ie) {
                    }
                }
            }
        }
    }

    /**
     * Server used for receiving/sending a block of data.
     * This is created to listen for requests from clients or
     * other DataNodes.  This small server does not use the
     * Hadoop IPC mechanism.
     */
    class DataXceiveServer implements Runnable {
        boolean shouldListen = true;
        ServerSocket ss;

        public DataXceiveServer(ServerSocket ss) {
            this.ss = ss;
        }

        /**
         */
        @Override
        public void run() {
            try {
                while (shouldListen) {
                    Socket s = ss.accept();
                    //s.setSoTimeout(READ_TIMEOUT);
                    new Daemon(new DataXceiver(s)).start();
                }
                ss.close();
            } catch (IOException ie) {
                LOGGER.info("Exiting DataXceiveServer due to " + ie.toString());
            }
        }

        public void kill() {
            this.shouldListen = false;
            try {
                this.ss.close();
            } catch (IOException iex) {
            }
        }
    }

    /**
     * Thread for processing incoming/outgoing data stream
     */
    class DataXceiver implements Runnable {
        Socket s;

        public DataXceiver(Socket s) {
            this.s = s;
        }

        /**
         * Read/write data from/to the DataXceiveServer.
         */
        @Override
        public void run() {
            try {
                DataInputStream in = new DataInputStream(new BufferedInputStream(s.getInputStream()));
                try {
                    byte op = (byte) in.read();
                    if (op == OP_WRITE_BLOCK) {
                        //
                        // Read in the header
                        //
                        DataOutputStream reply = new DataOutputStream(new BufferedOutputStream(s.getOutputStream()));
                        try {
                            boolean shouldReportBlock = in.readBoolean();
                            Block b = new Block();
                            b.readFields(in);
                            int numTargets = in.readInt();
                            if (numTargets <= 0) {
                                throw new IOException("Mislabelled incoming datastream.");
                            }
                            DatanodeInfo[] targets = new DatanodeInfo[numTargets];
                            for (int i = 0; i < targets.length; i++) {
                                DatanodeInfo tmp = new DatanodeInfo();
                                tmp.readFields(in);
                                targets[i] = tmp;
                            }
                            byte encodingType = (byte) in.read();
                            long len = in.readLong();

                            //
                            // Make sure curTarget is equal to this machine
                            //
                            DatanodeInfo curTarget = targets[0];

                            //
                            // Track all the places we've successfully written the block
                            //
                            Vector mirrors = new Vector();

                            //
                            // Open local disk out
                            //
                            DataOutputStream out = new DataOutputStream(new BufferedOutputStream(data.writeToBlock(b)));
                            InetSocketAddress mirrorTarget = null;
                            try {
                                //
                                // Open network conn to backup machine, if 
                                // appropriate
                                //
                                DataInputStream in2 = null;
                                DataOutputStream out2 = null;
                                if (targets.length > 1) {
                                    // Connect to backup machine
                                    mirrorTarget = createSocketAddr(targets[1].getName().toString());
                                    try {
                                        Socket s2 = new Socket();
                                        s2.connect(mirrorTarget, READ_TIMEOUT);
                                        s2.setSoTimeout(READ_TIMEOUT);
                                        out2 = new DataOutputStream(new BufferedOutputStream(s2.getOutputStream()));
                                        in2 = new DataInputStream(new BufferedInputStream(s2.getInputStream()));

                                        // Write connection header
                                        out2.write(OP_WRITE_BLOCK);
                                        out2.writeBoolean(shouldReportBlock);
                                        b.write(out2);
                                        out2.writeInt(targets.length - 1);
                                        for (int i = 1; i < targets.length; i++) {
                                            targets[i].write(out2);
                                        }
                                        out2.write(encodingType);
                                        out2.writeLong(len);
                                    } catch (IOException ie) {
                                        if (out2 != null) {
                                            try {
                                                out2.close();
                                                in2.close();
                                            } catch (IOException out2close) {
                                            } finally {
                                                out2 = null;
                                                in2 = null;
                                            }
                                        }
                                    }
                                }

                                //
                                // Process incoming data, copy to disk and
                                // maybe to network.
                                //
                                try {
                                    boolean anotherChunk = len != 0;
                                    byte[] buf = new byte[BUFFER_SIZE];

                                    while (anotherChunk) {
                                        while (len > 0) {
                                            int bytesRead = in.read(buf, 0, (int) Math.min(buf.length, len));
                                            if (bytesRead < 0) {
                                                throw new EOFException("EOF reading from " + s.toString());
                                            }
                                            if (bytesRead > 0) {
                                                try {
                                                    out.write(buf, 0, bytesRead);
                                                } catch (IOException iex) {
                                                    shutdown();
                                                    throw iex;
                                                }
                                                if (out2 != null) {
                                                    try {
                                                        out2.write(buf, 0, bytesRead);
                                                    } catch (IOException out2e) {
                                                        //
                                                        // If stream-copy fails, continue 
                                                        // writing to disk.  We shouldn't 
                                                        // interrupt client write.
                                                        //
                                                        try {
                                                            out2.close();
                                                            in2.close();
                                                        } catch (IOException out2close) {
                                                        } finally {
                                                            out2 = null;
                                                            in2 = null;
                                                        }
                                                    }
                                                }
                                                len -= bytesRead;
                                            }
                                        }

                                        if (encodingType == RUNLENGTH_ENCODING) {
                                            anotherChunk = false;
                                        } else if (encodingType == CHUNKED_ENCODING) {
                                            len = in.readLong();
                                            if (out2 != null) {
                                                out2.writeLong(len);
                                            }
                                            if (len == 0) {
                                                anotherChunk = false;
                                            }
                                        }
                                    }

                                    if (out2 == null) {
                                        LOGGER.info("Received block " + b + " from " + s.getInetAddress());
                                    } else {
                                        out2.flush();
                                        long complete = in2.readLong();
                                        if (complete != WRITE_COMPLETE) {
                                            LOGGER.info("Conflicting value for WRITE_COMPLETE: " + complete);
                                        }
                                        LocatedBlock newLB = new LocatedBlock();
                                        newLB.readFields(in2);
                                        DatanodeInfo[] mirrorsSoFar = newLB.getLocations();
                                        for (int k = 0; k < mirrorsSoFar.length; k++) {
                                            mirrors.add(mirrorsSoFar[k]);
                                        }
                                        LOGGER.info("Received block " + b + " from " + s.getInetAddress() + " and mirrored to " + mirrorTarget);
                                    }
                                } finally {
                                    if (out2 != null) {
                                        out2.close();
                                        in2.close();
                                    }
                                }
                            } finally {
                                try {
                                    out.close();
                                } catch (IOException iex) {
                                    shutdown();
                                    throw iex;
                                }
                            }
                            data.finalizeBlock(b);

                            // 
                            // Tell the namenode that we've received this block 
                            // in full, if we've been asked to.  This is done
                            // during NameNode-directed block transfers, but not
                            // client writes.
                            //
                            if (shouldReportBlock) {
                                synchronized (receivedBlockList) {
                                    receivedBlockList.add(b);
                                    receivedBlockList.notifyAll();
                                }
                            }

                            //
                            // Tell client job is done, and reply with
                            // the new LocatedBlock.
                            //
                            reply.writeLong(WRITE_COMPLETE);
                            mirrors.add(curTarget);
                            LocatedBlock newLB = new LocatedBlock(b, (DatanodeInfo[]) mirrors.toArray(new DatanodeInfo[mirrors.size()]));
                            newLB.write(reply);
                        } finally {
                            reply.close();
                        }
                    } else if (op == OP_READ_BLOCK || op == OP_READSKIP_BLOCK) {
                        //
                        // Read in the header
                        //
                        Block b = new Block();
                        b.readFields(in);

                        long toSkip = 0;
                        if (op == OP_READSKIP_BLOCK) {
                            toSkip = in.readLong();
                        }

                        //
                        // Open reply stream
                        //
                        DataOutputStream out = new DataOutputStream(new BufferedOutputStream(s.getOutputStream()));
                        try {
                            //
                            // Write filelen of -1 if error
                            //
                            if (!data.isValidBlock(b)) {
                                out.writeLong(-1);
                            } else {
                                //
                                // Get blockdata from disk
                                //
                                long len = data.getLength(b);
                                DataInputStream in2 = new DataInputStream(data.getBlockData(b));
                                out.writeLong(len);

                                if (op == OP_READSKIP_BLOCK) {
                                    if (toSkip > len) {
                                        toSkip = len;
                                    }
                                    long amtSkipped = 0;
                                    try {
                                        amtSkipped = in2.skip(toSkip);
                                    } catch (IOException iex) {
                                        shutdown();
                                        throw iex;
                                    }
                                    out.writeLong(amtSkipped);
                                }

                                byte[] buf = new byte[BUFFER_SIZE];
                                try {
                                    int bytesRead = 0;
                                    try {
                                        bytesRead = in2.read(buf);
                                    } catch (IOException iex) {
                                        shutdown();
                                        throw iex;
                                    }
                                    while (bytesRead >= 0) {
                                        out.write(buf, 0, bytesRead);
                                        len -= bytesRead;
                                        try {
                                            bytesRead = in2.read(buf);
                                        } catch (IOException iex) {
                                            shutdown();
                                            throw iex;
                                        }
                                    }
                                } catch (SocketException se) {
                                    // This might be because the reader
                                    // closed the stream early
                                } finally {
                                    try {
                                        in2.close();
                                    } catch (IOException iex) {
                                        shutdown();
                                        throw iex;
                                    }
                                }
                            }
                            LOGGER.info("Served block " + b + " to " + s.getInetAddress());
                        } finally {
                            out.close();
                        }
                    } else {
                        while (op >= 0) {
                            System.out.println("Faulty op: " + op);
                            op = (byte) in.read();
                        }
                        throw new IOException("Unknown opcode for incoming data stream");
                    }
                } finally {
                    in.close();
                }
            } catch (IOException ie) {
                LOGGER.warn("DataXCeiver", ie);
            } finally {
                try {
                    s.close();
                } catch (IOException ie2) {
                }
            }
        }
    }

    /**
     * Used for transferring a block of data.  This class
     * sends a piece of data to another DataNode.
     */
    class DataTransfer implements Runnable {
        InetSocketAddress curTarget;
        DatanodeInfo[] targets;
        Block b;
        byte[] buf;

        /**
         * Connect to the first item in the target list.  Pass along the
         * entire target list, the block, and the data.
         */
        public DataTransfer(DatanodeInfo[] targets, Block b) throws IOException {
            this.curTarget = createSocketAddr(targets[0].getName().toString());
            this.targets = targets;
            this.b = b;
            this.buf = new byte[BUFFER_SIZE];
        }

        /**
         * Do the deed, write the bytes
         */
        @Override
        public void run() {
            xmitsInProgress++;
            try {
                Socket s = new Socket();
                s.connect(curTarget, READ_TIMEOUT);
                s.setSoTimeout(READ_TIMEOUT);
                DataOutputStream out = new DataOutputStream(new BufferedOutputStream(s.getOutputStream()));
                try {
                    long filelen = data.getLength(b);
                    DataInputStream in = new DataInputStream(new BufferedInputStream(data.getBlockData(b)));
                    try {
                        //
                        // Header info
                        //
                        out.write(OP_WRITE_BLOCK);
                        out.writeBoolean(true);
                        b.write(out);
                        out.writeInt(targets.length);
                        for (int i = 0; i < targets.length; i++) {
                            targets[i].write(out);
                        }
                        out.write(RUNLENGTH_ENCODING);
                        out.writeLong(filelen);

                        //
                        // Write the data
                        //
                        while (filelen > 0) {
                            int bytesRead = in.read(buf, 0, (int) Math.min(filelen, buf.length));
                            out.write(buf, 0, bytesRead);
                            filelen -= bytesRead;
                        }
                    } finally {
                        in.close();
                    }
                } finally {
                    out.close();
                }
                LOGGER.info("Transmitted block " + b + " to " + curTarget);
            } catch (IOException ie) {
                LOGGER.warn("Failed to transfer " + b + " to " + curTarget, ie);
            } finally {
                xmitsInProgress--;
            }
        }
    }

    /**
     * No matter what kind of exception we get, keep retrying to offerService().
     * That's the loop that connects to the NameNode and provides basic DataNode
     * functionality.
     * <p>
     * Only stop when "shouldRun" is turned off (which can only happen at shutdown).
     */
    @Override
    public void run() {
        LOGGER.info("Starting DataNode in: " + data.data);
        while (shouldRun) {
            try {
                offerService();
            } catch (Exception ex) {
                LOGGER.info("Exception: " + ex);
                if (shouldRun) {
                    LOGGER.info("Lost connection to namenode.  Retrying...");
                    try {
                        Thread.sleep(5000);
                    } catch (InterruptedException ie) {
                    }
                }
            }
        }
        LOGGER.info("Finishing DataNode in: " + data.data);
    }

    /**
     * Start datanode daemons.
     * Start a datanode daemon for each comma separated data directory
     * specified in property dfs.data.dir
     */
    public static void run(Configuration conf) throws IOException {
        String[] dataDirs = conf.getStrings("dfs.data.dir");
        subThreadList = new Vector(dataDirs.length);
        for (int i = 0; i < dataDirs.length; i++) {
            DataNode dn = makeInstanceForDir(dataDirs[i], conf);
            if (dn != null) {
                Thread t = new Thread(dn, "DataNode: " + dataDirs[i]);
                // needed for JUnit testing
                t.setDaemon(true);
                t.start();
                subThreadList.add(t);
            }
        }
    }

    /**
     * Start datanode daemons.
     * Start a datanode daemon for each comma separated data directory
     * specified in property dfs.data.dir and wait for them to finish.
     * If this thread is specifically interrupted, it will stop waiting.
     */
    private static void runAndWait(Configuration conf) throws IOException {
        run(conf);

        //  Wait for sub threads to exit
        for (Iterator iterator = subThreadList.iterator(); iterator.hasNext(); ) {
            Thread threadDataNode = (Thread) iterator.next();
            try {
                threadDataNode.join();
            } catch (InterruptedException e) {
                if (Thread.currentThread().isInterrupted()) {
                    // did someone knock?
                    return;
                }
            }
        }
    }

    /**
     * Make an instance of DataNode after ensuring that given data directory
     * (and parent directories, if necessary) can be created.
     * @param dataDir where the new DataNode instance should keep its files.
     * @param conf    Configuration instance to use.
     * @return DataNode instance for given data dir and conf, or null if directory
     * cannot be created.
     * @throws IOException
     */
    static DataNode makeInstanceForDir(String dataDir, Configuration conf) throws IOException {
        DataNode dn = null;
        File data = new File(dataDir);
        data.mkdirs();
        if (!data.isDirectory()) {
            LOGGER.warn("Can't start DataNode in non-directory: " + dataDir);
            return null;
        } else {
            dn = new DataNode(conf, dataDir);
        }
        return dn;
    }

    @Override
    public String toString() {
        return "DataNode{" +
                "data=" + data +
                ", localName='" + localName + "'" +
                ", xmitsInProgress=" + xmitsInProgress +
                "}";
    }

    public static void main(String[] args) throws IOException {
        runAndWait(new Configuration());
    }
}
