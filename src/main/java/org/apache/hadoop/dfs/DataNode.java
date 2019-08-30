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
 * DataNode是一个类(和程序)，它为DFS部署存储一组块。<br/>
 * 一个部署可以有一个或多个datanode。<br/>
 * 每个DataNode定期与一个NameNode通信。<br/>
 * 它还不时地与客户机代码和其他数据节点通信。<br/>
 * datanode存储一系列命名块。<br/>
 * DataNode允许客户端代码读取这些块，或者编写新的块数据。<br/>
 * DataNode还可以响应来自其NameNode的指令，删除块或将块复制到/从其他DataNode。<br/>
 * DataNode只维护一个关键表:block->字节流(BLOCK_SIZE或更小)<br/>
 * 此信息存储在本地磁盘上。<br/>
 * DataNode在启动时以及之后经常向NameNode报告表的内容。<br/>
 * 数据节点将它们的生命花费在无休止的循环中，要求NameNode做一些事情。<br/>
 * NameNode不能直接连接到数据节点;NameNode只返回由DataNode调用的函数的值。<br/>
 * 数据节点维护一个开放的服务器套接字，以便客户机代码或其他数据节点能够读取/写入数据。<br/>
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
     * 创建给定配置和dataDir的DataNode。<br/>
     * “dataDir”是存储块的地方。
     */
    public DataNode(Configuration conf, String datadir) throws IOException {
        this(InetAddress.getLocalHost().getHostName(), new File(datadir), createSocketAddr(Constants.FS_DEFAULT_NAME), conf);
    }

    /**
     * 还可以使用显式给定的配置信息创建DataNode。
     * @param machineName  当前机器域名
     * @param datadir      datanode存储目录(其中一个具体的目录)
     * @param nameNodeAddr namenode地址
     * @param conf         配置类
     * @throws IOException
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
     * 关闭datanode的这个实例。<br/>
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
     * DataNode的主回路。<br/>
     * 运行到关机，永远调用远程NameNode函数。
     */
    public void offerService() throws Exception {
        long wakeups = 0;
        long lastHeartbeat = 0, lastBlockReport = 0;
        long sendStart = System.currentTimeMillis();
        int heartbeatsSent = 0;
        LOGGER.info("using BLOCKREPORT_INTERVAL of " + blockReportInterval + " msec");

        // 现在循环很长时间…
        while (shouldRun) {
            long now = System.currentTimeMillis();

            // 每隔一段时间，发送心跳或阻塞报告
            synchronized (receivedBlockList) {
                if (now - lastHeartbeat > HEARTBEAT_INTERVAL) {
                    //所有心跳消息包括以下信息:
                    //——Datanode名称
                    //——数据传输端口
                    //——总容量
                    //——剩余字节
                    namenode.sendHeartbeat(localName, data.getCapacity(), data.getRemaining());
                    LOGGER.info("Just sent heartbeat, with name " + localName);
                    lastHeartbeat = now;
                }
                if (now - lastBlockReport > blockReportInterval) {
                    // 发送最新的区块信息报告，如果计时器已经过期。
                    // 获取一个本地块的列表，这些块已经过时，可以安全地GC'ed。
                    Block[] toDelete = namenode.blockReport(localName, data.getBlockReport());
                    data.invalidate(toDelete);
                    lastBlockReport = now;
                    continue;
                }
                if (receivedBlockList.size() > 0) {
                    // 将新接收到的blockids发送到namenode
                    Block[] blockArray = receivedBlockList.toArray(new Block[receivedBlockList.size()]);
                    receivedBlockList.removeAllElements();
                    namenode.blockReceived(localName, blockArray);
                }

                //只在启动静默期之后执行块操作(传输、删除)。
                //假设所有的数据节点都将一起启动，但是namenode可能在此之前已经启动了一段时间。
                //(在网络中断的情况下尤其如此。)
                //因此，等待一段时间将连接时间传递到第一个块传输。
                //否则，我们会不必要地转移很多块。
                if (now - sendStart > datanodeStartupPeriod) {
                    // 检查这个datanode是否应该执行来自namenode的任何块指令。
                    BlockCommand cmd = namenode.getBlockwork(localName, xmitsInProgress);
                    if (cmd != null && cmd.transferBlocks()) {
                        // 将一个块的副本发送到另一个datanode
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
                        // 一些本地块已经过时，可以安全地进行垃圾收集。
                        data.invalidate(cmd.getBlocks());
                    }
                }

                // 没有工作可做;休眠，直到hearbeat计时器超时，或者工作到达，然后再重复。
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
     * 用于接收/发送数据块的服务器。<br/>
     * 创建它是为了侦听来自客户机或其他datanode的请求。<br/>
     * 这个小服务器不使用Hadoop IPC机制。
     */
    public class DataXceiveServer implements Runnable {
        boolean shouldListen = true;
        ServerSocket ss;

        public DataXceiveServer(ServerSocket ss) {
            this.ss = ss;
        }

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
     * 处理传入/传出数据流的线程
     */
    public class DataXceiver implements Runnable {
        Socket s;

        public DataXceiver(Socket s) {
            this.s = s;
        }

        /**
         * 从/向DataXceiveServer读取/写入数据。
         */
        @Override
        public void run() {
            try {
                DataInputStream in = new DataInputStream(new BufferedInputStream(s.getInputStream()));
                try {
                    byte op = (byte) in.read();
                    if (op == OP_WRITE_BLOCK) {
                        // 在页眉中读取
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

                            // 确保curTarget等于这台机器
                            DatanodeInfo curTarget = targets[0];

                            // 跟踪我们成功地编写了块的所有位置
                            Vector<DatanodeInfo> mirrors = new Vector<DatanodeInfo>();

                            // 打开本地磁盘
                            DataOutputStream out = new DataOutputStream(new BufferedOutputStream(data.writeToBlock(b)));
                            InetSocketAddress mirrorTarget = null;
                            try {
                                // 打开网络连接到备份机，如果合适的话
                                DataInputStream in2 = null;
                                DataOutputStream out2 = null;
                                if (targets.length > 1) {
                                    // 连接到备份机
                                    mirrorTarget = createSocketAddr(targets[1].getName().toString());
                                    try {
                                        Socket s2 = new Socket();
                                        s2.connect(mirrorTarget, READ_TIMEOUT);
                                        s2.setSoTimeout(READ_TIMEOUT);
                                        out2 = new DataOutputStream(new BufferedOutputStream(s2.getOutputStream()));
                                        in2 = new DataInputStream(new BufferedInputStream(s2.getInputStream()));

                                        // 编写连接头
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

                                // 处理传入的数据，复制到磁盘，或者网络。
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
                                                        // 如果流复制失败，请继续写入磁盘。
                                                        // 我们不应该中断客户端写。
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

                            // 如果要求的话，告诉namenode我们已经完整地接收了这个块。
                            // 这是在namenode定向的块传输期间完成的，但不是在客户机写入期间。
                            if (shouldReportBlock) {
                                synchronized (receivedBlockList) {
                                    receivedBlockList.add(b);
                                    receivedBlockList.notifyAll();
                                }
                            }

                            // 告诉客户任务已经完成，并使用新的LocatedBlock进行回复。
                            reply.writeLong(WRITE_COMPLETE);
                            mirrors.add(curTarget);
                            LocatedBlock newLB = new LocatedBlock(b, mirrors.toArray(new DatanodeInfo[mirrors.size()]));
                            newLB.write(reply);
                        } finally {
                            reply.close();
                        }
                    } else if (op == OP_READ_BLOCK || op == OP_READSKIP_BLOCK) {
                        // 在页眉中读取
                        Block b = new Block();
                        b.readFields(in);

                        long toSkip = 0;
                        if (op == OP_READSKIP_BLOCK) {
                            toSkip = in.readLong();
                        }

                        // 打开回复流
                        DataOutputStream out = new DataOutputStream(new BufferedOutputStream(s.getOutputStream()));
                        try {
                            // 如果错误，写filelen (-1)
                            if (!data.isValidBlock(b)) {
                                out.writeLong(-1);
                            } else {
                                // 从磁盘获取块数据
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
                                    // 这可能是因为读者提前关闭了流
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
     * 用于传输数据块。<br/>
     * 该类向另一个DataNode发送一段数据。
     */
    public class DataTransfer implements Runnable {
        InetSocketAddress curTarget;
        DatanodeInfo[] targets;
        Block b;
        byte[] buf;

        /**
         * 连接到目标列表中的第一项。<br/>
         * 传递整个目标列表、块和数据。
         */
        public DataTransfer(DatanodeInfo[] targets, Block b) throws IOException {
            this.curTarget = createSocketAddr(targets[0].getName().toString());
            this.targets = targets;
            this.b = b;
            this.buf = new byte[BUFFER_SIZE];
        }

        /**
         * 做契约，写字节
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
                        // 标题信息
                        out.write(OP_WRITE_BLOCK);
                        out.writeBoolean(true);
                        b.write(out);
                        out.writeInt(targets.length);
                        for (int i = 0; i < targets.length; i++) {
                            targets[i].write(out);
                        }
                        out.write(RUNLENGTH_ENCODING);
                        out.writeLong(filelen);

                        // 写入数据
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
     * 无论我们得到什么样的异常，请继续重试offerService()。<br/>
     * 这个循环连接到NameNode并提供基本的DataNode功能。<br/>
     * 只有当“shouldRun”关闭时才会停止(只有在关机时才会发生)。
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
     * datanode守护进程开始。<br/>
     * 为属性dfs.data.dir中指定的每个逗号分隔的数据目录启动datanode守护进程
     */
    public static void run(Configuration conf) throws IOException {
        String[] dataDirs = conf.getStrings("dfs.data.dir");
        subThreadList = new Vector(dataDirs.length);
        for (int i = 0; i < dataDirs.length; i++) {
            DataNode dn = makeInstanceForDir(dataDirs[i], conf);
            if (dn != null) {
                Thread t = new Thread(dn, "DataNode: " + dataDirs[i]);
                t.setDaemon(true);
                t.start();
                subThreadList.add(t);
            }
        }
    }

    /**
     * datanode守护进程开始。<br/>
     * 为属性dfs.data中指定的每个逗号分隔的数据目录启动datanode守护进程。并等待他们完成。<br/>
     * 如果这个线程被特别中断，它将停止等待。
     */
    private static void runAndWait(Configuration conf) throws IOException {
        // 根据配置的data目录添加守护线程到subThreadList
        run(conf);
        //  等待子线程退出
        for (Iterator<Thread> iterator = subThreadList.iterator(); iterator.hasNext(); ) {
            Thread threadDataNode = iterator.next();
            try {
                threadDataNode.join();
            } catch (InterruptedException e) {
                if (Thread.currentThread().isInterrupted()) {
                    return;
                }
            }
        }
    }

    /**
     * 在确保可以创建给定的数据目录(以及父目录，如果必要的话)之后，创建一个DataNode实例。
     * @param dataDir 新DataNode实例应该将其文件保存在何处。
     * @param conf    要使用的配置实例。
     * @return 指定数据目录和conf的DataNode实例，如果无法创建目录，则为null。
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
