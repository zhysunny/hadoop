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
import org.apache.hadoop.util.Constants;
import org.apache.hadoop.util.Daemon;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.*;

/**
 * fsnamessystem为DataNode做实际的簿记工作。<br/>
 * 它跟踪几个重要的表。<br/>
 * 1)有效fsname——>块列表(保存在磁盘上，已记录)<br/>
 * 2)所有有效块的集合(倒排#1)<br/>
 * 3)块 ——> 机器列表(保存在内存中，根据报表动态重建)<br/>
 * 4)机器 ——> 块列表(倒排#2)<br/>
 * 5)更新心跳机的LRU缓存
 * @author 章云
 * @date 2019/7/31 10:54
 */
public class FSNamesystem implements FSConstants {
    private static final Logger LOGGER = LoggerFactory.getLogger(FSNamesystem.class);

    /**
     * 存储正确的文件名层次结构
     */
    FSDirectory dir;

    /**
     * 存储块——>datanode(s)映射。仅在响应客户端发送的信息时更新。
     */
    TreeMap<Block, TreeSet<DatanodeInfo>> blocksMap = new TreeMap<Block, TreeSet<DatanodeInfo>>();

    /**
     * 存储datanode——>块映射。通过存储一组按名称排序的datanode信息对象来完成。仅在响应客户端发送的信息时更新。
     */
    TreeMap<UTF8, DatanodeInfo> datanodeMap = new TreeMap<UTF8, DatanodeInfo>();

    /**
     * 为每个命名的机器保存一个向量。该向量包含最近已失效的块，并且被认为存在于所讨论的机器上。
     */
    TreeMap<UTF8, Vector<Block>> recentInvalidateSets = new TreeMap<UTF8, Vector<Block>>();

    /**
     * 为每个命名节点保留一个树集。每个树集包含在该位置的“额外”块的列表。我们最终会删除这些额外的内容。
     */
    TreeMap<UTF8, TreeSet<Block>> excessReplicateMap = new TreeMap<UTF8, TreeSet<Block>>();

    /**
     * 跟踪正在创建的文件，以及组成这些文件的块。
     */
    TreeMap<UTF8, Vector<Block>> pendingCreates = new TreeMap<UTF8, Vector<Block>>();

    /**
     * 跟踪作为挂起创建的一部分的块
     */
    TreeSet<Block> pendingCreateBlocks = new TreeSet<Block>();

    /**
     * 总使用量统计
     */
    long totalCapacity = 0, totalRemaining = 0;

    /**
     * 随机数对象
     */
    Random r = new Random();

    /**
     * 存储一组按心跳排序的DatanodeInfo对象
     */
    TreeSet<DatanodeInfo> heartbeats = new TreeSet<DatanodeInfo>(new Comparator<DatanodeInfo>() {
        @Override
        public int compare(DatanodeInfo d1, DatanodeInfo d2) {
            // datanode最后修改时间升序排序
            long lu1 = d1.lastUpdate();
            long lu2 = d2.lastUpdate();
            if (lu1 < lu2) {
                return -1;
            } else if (lu1 > lu2) {
                return 1;
            } else {
                return d1.getName().compareTo(d2.getName());
            }
        }
    });

    /**
     * 存储一组需要复制1次或多次的块。
     */
    private TreeSet<Block> neededReplications = new TreeSet<Block>();
    /**
     * 存储等待复制的副本。
     */
    private TreeSet<Block> pendingReplications = new TreeSet<Block>();
    /**
     * 用于处理锁租赁
     */
    private TreeMap<UTF8, Lease> leases = new TreeMap<UTF8, Lease>();
    /**
     * 锁排序
     */
    private TreeSet<Lease> sortedLeases = new TreeSet<Lease>();
    /**
     * 线程对象，检查我们是否从所有客户端获得心跳。
     */
    HeartbeatMonitor hbmon = null;
    /**
     * 锁监控
     */
    LeaseMonitor lmon = null;
    /**
     * 守护线程，心跳监控，锁监控
     */
    Daemon hbthread = null, lmthread = null;
    /**
     * 文件系统是否正在运行中
     */
    boolean fsRunning = true;
    /**
     * 系统开始时间，程序启动时间
     */
    long systemStart = 0;
    /**
     * 配置类
     */
    private Configuration conf;
    /**
     * DESIRED_REPLICATION是我们在任何时候都有多少个副本
     */
    private int desiredReplication;
    /**
     * 对于单个块，我们应该允许的最大副本数
     */
    private int maxReplication;
    /**
     * 给定节点一次应该有多少输出复制流
     */
    private int maxReplicationStreams;
    /**
     * MIN_REPLICATION是我们需要多少个副本，否则我们不允许写入
     */
    private int minReplication;
    /**
     * HEARTBEAT_RECHECK是datanode发送它的心跳频率(毫秒)(默认1秒)
     */
    private int heartBeatRecheck;

    /**
     * 实例化
     * @param dir  文件系统目录状态存储的位置
     * @param conf
     * @throws IOException
     */
    public FSNamesystem(File dir, Configuration conf) throws IOException {
        this.dir = new FSDirectory(dir);
        this.hbthread = new Daemon(new HeartbeatMonitor());
        this.lmthread = new Daemon(new LeaseMonitor());
        hbthread.start();
        lmthread.start();
        this.systemStart = System.currentTimeMillis();
        this.conf = conf;
        this.desiredReplication = Constants.DFS_REPLICATION;
        this.maxReplication = desiredReplication;
        this.maxReplicationStreams = Constants.DFS_MAX_REPL_STREAMS;
        this.minReplication = 1;
        this.heartBeatRecheck = 1000;
    }

    /**
     * 关闭这个文件系统管理器。<br/>
     * 导致heartbeat和租约守护进程停止;短暂地等待它们完成，但短暂的超时将控制权返回给调用者。
     */
    public void close() {
        synchronized (this) {
            fsRunning = false;
        }
        try {
            hbthread.join(3000);
        } catch (InterruptedException ie) {
        } finally {
            // 使用finally来确保我们还在等待租赁守护进程
            try {
                lmthread.join(3000);
            } catch (InterruptedException ie) {
            }
        }
    }

    /////////////////////////////////////////////////////////
    //
    // 这些方法由HadoopFS客户机调用
    //
    /////////////////////////////////////////////////////////

    /**
     * 客户端想要打开给定的文件名。<br/>
     * 返回(block,machineArray)对的列表。<br/>
     * 列表中唯一块的序列表示组成文件名的所有块。<br/>
     * 客户应从机器射线中随机选择一台机器。
     */
    public Object[] open(UTF8 src) {
        Object[] results = null;
        Block[] blocks = dir.getFile(src);
        if (blocks != null) {
            results = new Object[2];
            DatanodeInfo[][] machineSets = new DatanodeInfo[blocks.length][];
            for (int i = 0; i < blocks.length; i++) {
                TreeSet<DatanodeInfo> containingNodes = blocksMap.get(blocks[i]);
                if (containingNodes == null) {
                    machineSets[i] = new DatanodeInfo[0];
                } else {
                    machineSets[i] = new DatanodeInfo[containingNodes.size()];
                    int j = 0;
                    for (Iterator<DatanodeInfo> it = containingNodes.iterator(); it.hasNext(); j++) {
                        machineSets[i][j] = it.next();
                    }
                }
            }
            results[0] = blocks;
            results[1] = machineSets;
        }
        return results;
    }

    /**
     * 客户端希望为指定的文件名创建一个新块。<br/>
     * 返回一个由块和一组机器组成的数组。<br/>
     * 这个列表中的第一个应该是客户机写入数据的地方。<br/>
     * 必须在连接到第一个datanode时提供列表中的后续项。
     * @return 返回一个由块和一组机器组成的数组，如果src对于创建无效则返回null(基于{@link FSDirectory#isValidToCreate(UTF8)})。
     */
    public synchronized Object[] startFile(UTF8 src, UTF8 holder, UTF8 clientMachine, boolean overwrite) {
        Object[] results = null;
        if (pendingCreates.get(src) == null) {
            boolean fileValid = dir.isValidToCreate(src);
            if (overwrite && !fileValid) {
                delete(src);
                fileValid = true;
            }
            if (fileValid) {
                results = new Object[2];
                // 获取复制目标数组
                DatanodeInfo[] targets = chooseTargets(this.desiredReplication, null, clientMachine);
                if (targets.length < this.minReplication) {
                    LOGGER.warn("Target-length is " + targets.length + ", below MIN_REPLICATION (" + this.minReplication + ")");
                    return null;
                }
                // 为这个挂起的文件保留空间
                pendingCreates.put(src, new Vector());
                synchronized (leases) {
                    Lease lease = leases.get(holder);
                    if (lease == null) {
                        lease = new Lease(holder);
                        leases.put(holder, lease);
                        sortedLeases.add(lease);
                    } else {
                        sortedLeases.remove(lease);
                        lease.renew();
                        sortedLeases.add(lease);
                    }
                    lease.startedCreate(src);
                }
                // 创建下一个块
                results[0] = allocateBlock(src);
                results[1] = targets;
            } else {
                LOGGER.warn("Cannot start file because it is invalid. src=" + src);
            }
        } else {
            LOGGER.warn("Cannot start file because pendingCreates is non-null. src=" + src);
        }
        return results;
    }

    /**
     * 客户端想要为指定的文件名获得一个额外的块(正在被写入到)。<br/>
     * 返回一个由块和一组机器组成的数组。<br/>
     * 这个列表中的第一个应该是客户机写入数据的地方。<br/>
     * 必须在连接到第一个datanode时提供列表中的后续项。<br/>
     * 确保前面的块已经被datanode报告并被复制。<br/>
     * 如果我们希望客户端“稍后重试”，将返回一个空的2-elt数组。
     */
    public synchronized Object[] getAdditionalBlock(UTF8 src, UTF8 clientMachine) {
        Object[] results = null;
        if (dir.getFile(src) == null && pendingCreates.get(src) != null) {
            results = new Object[2];
            // 如果我们失败了，坏事就会发生!
            if (checkFileProgress(src)) {
                // 获取复制目标数组
                DatanodeInfo[] targets = chooseTargets(this.desiredReplication, null, clientMachine);
                if (targets.length < this.minReplication) {
                    return null;
                }
                // 创建下一个块
                results[0] = allocateBlock(src);
                results[1] = targets;
            }
        }
        return results;
    }

    /**
     * 客户端希望释放给定的块
     */
    public synchronized boolean abandonBlock(Block b, UTF8 src) {
        // 从pending create列表中删除该块
        Vector<Block> pendingVector = pendingCreates.get(src);
        if (pendingVector != null) {
            for (Iterator<Block> it = pendingVector.iterator(); it.hasNext(); ) {
                Block cur = it.next();
                if (cur.compareTo(b) == 0) {
                    pendingCreateBlocks.remove(cur);
                    it.remove();
                    return true;
                }
            }
        }
        return false;
    }

    /**
     * 放弃正在处理的整个文件
     */
    public synchronized void abandonFileInProgress(UTF8 src) {
        internalReleaseCreate(src);
    }

    /**
     * 完成创建的文件，并使其可访问。<br/>
     * fsnamessystem将已经知道组成文件的块。<br/>
     * 在返回之前，我们要确保datanode已经报告了文件的所有块，并且正确地复制了它们。
     */
    public synchronized int completeFile(UTF8 src, UTF8 holder) {
        if (dir.getFile(src) != null || pendingCreates.get(src) == null) {
            LOGGER.info("Failed to complete " + src + "  because dir.getFile()==" + dir.getFile(src) + " and " + pendingCreates.get(src));
            return OPERATION_FAILED;
        } else if (!checkFileProgress(src)) {
            return STILL_WAITING;
        } else {
            Vector<Block> pendingVector = pendingCreates.get(src);
            Block[] pendingBlocks = pendingVector.toArray(new Block[pendingVector.size()]);
            // 我们有挂起的块，但是它们没有长度信息(因为它们是在数据写入之前分配的)。
            // 所以我们需要添加正确的长度信息到每个提醒- mjc -这是非常低效的!
            // 我们应该改进这一点!
            for (int i = 0; i < pendingBlocks.length; i++) {
                Block b = pendingBlocks[i];
                TreeSet<DatanodeInfo> containingNodes = blocksMap.get(b);
                DatanodeInfo node = containingNodes.first();
                for (Iterator<Block> it = node.getBlockIterator(); it.hasNext(); ) {
                    Block cur = it.next();
                    if (b.getBlockId() == cur.getBlockId()) {
                        b.setNumBytes(cur.getNumBytes());
                        break;
                    }
                }
            }
            // 现在我们可以将(名称、块)元组添加到文件系统中
            if (dir.addFile(src, pendingBlocks)) {
                // 该文件不再挂起
                pendingCreates.remove(src);
                for (int i = 0; i < pendingBlocks.length; i++) {
                    pendingCreateBlocks.remove(pendingBlocks[i]);
                }
                synchronized (leases) {
                    Lease lease = leases.get(holder);
                    if (lease != null) {
                        lease.completedCreate(src);
                        if (!lease.hasLocks()) {
                            leases.remove(holder);
                            sortedLeases.remove(lease);
                        }
                    }
                }
                //提醒-mjc-这应该只在我们等待几秒钟后才做。
                // namenode没有给datanode足够的时间来报告作为客户端写的一部分自动完成的复制块。
                //既然文件是真实的，我们需要确保复制这些块。
                for (int i = 0; i < pendingBlocks.length; i++) {
                    TreeSet<DatanodeInfo> containingNodes = blocksMap.get(pendingBlocks[i]);
                    if (containingNodes.size() < this.desiredReplication) {
                        synchronized (neededReplications) {
                            LOGGER.info("Completed file " + src + ", at holder " + holder + ".  There is/are only " + containingNodes.size() + " copies of block " + pendingBlocks[i] + ", so replicating up to " + this.desiredReplication);
                            neededReplications.add(pendingBlocks[i]);
                        }
                    }
                }
                return COMPLETE_SUCCESS;
            } else {
                System.out.println("AddFile() for " + src + " failed");
            }
            LOGGER.info("Dropped through on file add....");
        }

        return OPERATION_FAILED;
    }

    /**
     * 在给定的挂起文件名处分配一个块
     */
    synchronized Block allocateBlock(UTF8 src) {
        Block b = new Block();
        Vector<Block> v = pendingCreates.get(src);
        v.add(b);
        pendingCreateBlocks.add(b);
        return b;
    }

    /**
     * 检查所指示文件的块是否存在并已复制。<br/>
     * 如果没有，返回false。
     */
    synchronized boolean checkFileProgress(UTF8 src) {
        Vector<Block> v = pendingCreates.get(src);
        for (Iterator<Block> it = v.iterator(); it.hasNext(); ) {
            Block b = it.next();
            TreeSet<DatanodeInfo> containingNodes = blocksMap.get(b);
            if (containingNodes == null || containingNodes.size() < this.minReplication) {
                return false;
            }
        }
        return true;
    }

    ////////////////////////////////////////////////////////////////
    // 这里是如何处理块复制失败在客户端写:
    // ——与往常一样，客户机的写操作应该导致对k-machine序列的流备份写操作。
    // ——如果其中一台备份机器发生故障，不用担心。默默的失败。
    // ——在允许客户端关闭和完成文件之前，请确保已备份了这些块。
    // Namenode可能必须发出特定的备份命令来弥补早期的datanode故障。
    // 复制完成后，编辑名称空间并返回给客户机。
    ////////////////////////////////////////////////////////////////

    /**
     * 更改指定的文件名。
     */
    public boolean renameTo(UTF8 src, UTF8 dst) {
        return dir.renameTo(src, dst);
    }

    /**
     * 从命名空间中删除指定的文件名。<br/>
     * 这可能会使组成文件的一些块失效。
     */
    public synchronized boolean delete(UTF8 src) {
        Block[] deletedBlocks = dir.delete(src);
        if (deletedBlocks != null) {
            for (int i = 0; i < deletedBlocks.length; i++) {
                Block b = deletedBlocks[i];
                TreeSet<DatanodeInfo> containingNodes = blocksMap.get(b);
                if (containingNodes != null) {
                    for (Iterator<DatanodeInfo> it = containingNodes.iterator(); it.hasNext(); ) {
                        DatanodeInfo node = it.next();
                        Vector<Block> invalidateSet = recentInvalidateSets.get(node.getName());
                        if (invalidateSet == null) {
                            invalidateSet = new Vector<Block>();
                            recentInvalidateSets.put(node.getName(), invalidateSet);
                        }
                        invalidateSet.add(b);
                    }
                }
            }
        }
        return (deletedBlocks != null);
    }

    /**
     * 返回给定文件名是否存在
     */
    public boolean exists(UTF8 src) {
        if (dir.getFile(src) != null || dir.isDir(src)) {
            return true;
        } else {
            return false;
        }
    }

    /**
     * 给定名称是否为目录
     */
    public boolean isDir(UTF8 src) {
        return dir.isDir(src);
    }

    /**
     * 创建所有必要的目录
     */
    public boolean mkdirs(UTF8 src) {
        return dir.mkdirs(src);
    }

    /**
     * 找出一些可能包含给定(文件名、开始、len)元组引用的块的主机。
     */
    public UTF8[][] getDatanodeHints(UTF8 src, long start, long len) {
        if (start < 0 || len < 0) {
            return new UTF8[0][];
        }
        int startBlock = -1;
        int endBlock = -1;
        Block[] blocks = dir.getFile(src);
        if (blocks == null) {
            // no blocks
            return new UTF8[0][];
        }
        // 首先，找出范围在块列表中的位置。
        long startpos = start;
        long endpos = start + len;
        for (int i = 0; i < blocks.length; i++) {
            if (startpos >= 0) {
                startpos -= blocks[i].getNumBytes();
                if (startpos <= 0) {
                    startBlock = i;
                }
            }
            if (endpos >= 0) {
                endpos -= blocks[i].getNumBytes();
                if (endpos <= 0) {
                    endBlock = i;
                    break;
                }
            }
        }
        // 接下来，创建一个主机数组，其中可以找到每个块
        if (startBlock < 0 || endBlock < 0) {
            return new UTF8[0][];
        } else {
            UTF8[][] hosts = new UTF8[(endBlock - startBlock) + 1][];
            for (int i = startBlock; i <= endBlock; i++) {
                TreeSet<DatanodeInfo> containingNodes = blocksMap.get(blocks[i]);
                Vector<UTF8> v = new Vector<UTF8>();
                for (Iterator<DatanodeInfo> it = containingNodes.iterator(); it.hasNext(); ) {
                    DatanodeInfo cur = it.next();
                    v.add(cur.getHost());
                }
                hosts[i - startBlock] = v.toArray(new UTF8[v.size()]);
            }
            return hosts;
        }
    }

    /************************************************************
     * 租约管理单个客户端持有的所有锁。<br/>
     * 对于每个客户端，都有一个对应的租约，当客户端定期签入时，租约的时间戳将被更新。<br/>
     * 如果客户机死亡并允许其租约过期，则可以释放所有相应的锁。
     *************************************************************/
    public class Lease implements Comparable {
        public UTF8 holder;
        public long lastUpdate;
        TreeSet<UTF8> locks = new TreeSet<UTF8>();
        TreeSet<UTF8> creates = new TreeSet<UTF8>();

        public Lease(UTF8 holder) {
            this.holder = holder;
            renew();
        }

        public void renew() {
            this.lastUpdate = System.currentTimeMillis();
        }

        public boolean expired() {
            if (System.currentTimeMillis() - lastUpdate > LEASE_PERIOD) {
                return true;
            } else {
                return false;
            }
        }

        public void obtained(UTF8 src) {
            locks.add(src);
        }

        public void released(UTF8 src) {
            locks.remove(src);
        }

        public void startedCreate(UTF8 src) {
            creates.add(src);
        }

        public void completedCreate(UTF8 src) {
            creates.remove(src);
        }

        public boolean hasLocks() {
            return (locks.size() + creates.size()) > 0;
        }

        public void releaseLocks() {
            for (Iterator it = locks.iterator(); it.hasNext(); ) {
                UTF8 src = (UTF8) it.next();
                internalReleaseLock(src, holder);
            }
            locks.clear();
            for (Iterator it = creates.iterator(); it.hasNext(); ) {
                UTF8 src = (UTF8) it.next();
                internalReleaseCreate(src);
            }
            creates.clear();
        }

        @Override
        public String toString() {
            return "[Lease.  Holder: " + holder.toString() + ", heldlocks: " + locks.size() + ", pendingcreates: " + creates.size() + "]";
        }

        @Override
        public int compareTo(Object o) {
            Lease l1 = this;
            Lease l2 = (Lease) o;
            long lu1 = l1.lastUpdate;
            long lu2 = l2.lastUpdate;
            if (lu1 < lu2) {
                return -1;
            } else if (lu1 > lu2) {
                return 1;
            } else {
                return l1.holder.compareTo(l2.holder);
            }
        }

        @Override
        public boolean equals(Object o) {
            Lease l = (Lease) o;
            // lastUpdate和holder都相等，两个对象就相等
            return (this.compareTo(l) == 0);
        }

        @Override
        public int hashCode() {
            return Objects.hash(this.lastUpdate, this.holder);
        }
    }

    /******************************************************
     * LeaseMonitor检查已过期的租约，并处理它们。
     ******************************************************/
    public class LeaseMonitor implements Runnable {
        @Override
        public void run() {
            while (fsRunning) {
                synchronized (FSNamesystem.this) {
                    synchronized (leases) {
                        Lease top;
                        while ((sortedLeases.size() > 0) &&
                                ((top = sortedLeases.first()) != null)) {
                            if (top.expired()) {
                                top.releaseLocks();
                                leases.remove(top.holder);
                                LOGGER.info("Removing lease " + top + ", leases remaining: " + sortedLeases.size());
                                if (!sortedLeases.remove(top)) {
                                    LOGGER.info("Unknown failure trying to remove " + top + " from lease set.");
                                }
                            } else {
                                break;
                            }
                        }
                    }
                }
                try {
                    Thread.sleep(2000);
                } catch (InterruptedException ie) {
                }
            }
        }
    }

    /**
     * 获取给定文件上的锁(可能是独占的)
     */
    public synchronized int obtainLock(UTF8 src, UTF8 holder, boolean exclusive) {
        int result = dir.obtainLock(src, holder, exclusive);
        if (result == COMPLETE_SUCCESS) {
            synchronized (leases) {
                Lease lease = leases.get(holder);
                if (lease == null) {
                    lease = new Lease(holder);
                    leases.put(holder, lease);
                    sortedLeases.add(lease);
                } else {
                    sortedLeases.remove(lease);
                    lease.renew();
                    sortedLeases.add(lease);
                }
                lease.obtained(src);
            }
        }
        return result;
    }

    /**
     * 释放给定文件上的锁
     */
    public synchronized int releaseLock(UTF8 src, UTF8 holder) {
        int result = internalReleaseLock(src, holder);
        if (result == COMPLETE_SUCCESS) {
            synchronized (leases) {
                Lease lease = leases.get(holder);
                if (lease != null) {
                    lease.released(src);
                    if (!lease.hasLocks()) {
                        leases.remove(holder);
                        sortedLeases.remove(lease);
                    }
                }
            }
        }
        return result;
    }

    private int internalReleaseLock(UTF8 src, UTF8 holder) {
        return dir.releaseLock(src, holder);
    }

    private void internalReleaseCreate(UTF8 src) {
        Vector<Block> v = pendingCreates.remove(src);
        for (Iterator<Block> it = v.iterator(); it.hasNext(); ) {
            Block b = it.next();
            pendingCreateBlocks.remove(b);
        }
    }

    /**
     * 续签客户所持有的租约
     */
    public void renewLease(UTF8 holder) {
        synchronized (leases) {
            Lease lease = leases.get(holder);
            if (lease != null) {
                sortedLeases.remove(lease);
                lease.renew();
                sortedLeases.add(lease);
            }
        }
    }

    /**
     * 获取'src'上所有文件的列表。<br/>
     * Object[]数组存在，因此我们可以返回文件属性(即将实现)
     */
    public DFSFileInfo[] getListing(UTF8 src) {
        return dir.getListing(src);
    }

    /////////////////////////////////////////////////////////
    //
    // 这些方法由datanode调用
    //
    /////////////////////////////////////////////////////////

    /**
     * 给定节点已报告。这个方法应该:<br/>
     * 1)记录心跳，这样datanode就不会超时<br/>
     * 2)为将来的块分配调整使用状态
     */
    public synchronized void gotHeartbeat(UTF8 name, long capacity, long remaining) {
        synchronized (heartbeats) {
            synchronized (datanodeMap) {
                long capacityDiff = 0;
                long remainingDiff = 0;
                DatanodeInfo nodeinfo = datanodeMap.get(name);
                if (nodeinfo == null) {
                    LOGGER.info("Got brand-new heartbeat from " + name);
                    nodeinfo = new DatanodeInfo(name, capacity, remaining);
                    datanodeMap.put(name, nodeinfo);
                    capacityDiff = capacity;
                    remainingDiff = remaining;
                } else {
                    capacityDiff = capacity - nodeinfo.getCapacity();
                    remainingDiff = remaining - nodeinfo.getRemaining();
                    heartbeats.remove(nodeinfo);
                    nodeinfo.updateHeartbeat(capacity, remaining);
                }
                heartbeats.add(nodeinfo);
                totalCapacity += capacityDiff;
                totalRemaining += remainingDiff;
            }
        }
    }

    /**
     * 定期调用heartbeatCheck ()。
     */
    public class HeartbeatMonitor implements Runnable {
        @Override
        public void run() {
            while (fsRunning) {
                heartbeatCheck();
                try {
                    Thread.sleep(heartBeatRecheck);
                } catch (InterruptedException ie) {
                }
            }
        }
    }

    /**
     * 检查是否有任何过期的心跳，如果有，是否需要重新复制任何块。
     */
    synchronized void heartbeatCheck() {
        synchronized (heartbeats) {
            DatanodeInfo nodeInfo = null;
            while ((heartbeats.size() > 0) &&
                    ((nodeInfo = heartbeats.first()) != null) &&
                    (nodeInfo.lastUpdate() < System.currentTimeMillis() - EXPIRE_INTERVAL)) {
                LOGGER.info("Lost heartbeat for " + nodeInfo.getName());
                heartbeats.remove(nodeInfo);
                synchronized (datanodeMap) {
                    datanodeMap.remove(nodeInfo.getName());
                }
                totalCapacity -= nodeInfo.getCapacity();
                totalRemaining -= nodeInfo.getRemaining();
                Block[] deadblocks = nodeInfo.getBlocks();
                if (deadblocks != null) {
                    for (int i = 0; i < deadblocks.length; i++) {
                        removeStoredBlock(deadblocks[i], nodeInfo);
                    }
                }

                if (heartbeats.size() > 0) {
                    nodeInfo = heartbeats.first();
                }
            }
        }
    }

    /**
     * 给定节点报告它的所有块。<br/>
     * 使用此信息更新(machine—>blocklist)和(block—>machinelist)表。
     */
    public synchronized Block[] processReport(Block[] newReport, UTF8 name) {
        DatanodeInfo node = datanodeMap.get(name);
        if (node == null) {
            throw new IllegalArgumentException("Unexpected exception.  Received block report from node " + name + ", but there is no info for " + name);
        }
        // 根据新旧块报表之间的差异修改(block——>datanode)映射。
        int oldPos = 0, newPos = 0;
        Block[] oldReport = node.getBlocks();
        while (oldReport != null && newReport != null && oldPos < oldReport.length && newPos < newReport.length) {
            int cmp = oldReport[oldPos].compareTo(newReport[newPos]);
            if (cmp == 0) {
                // 什么都不做，block块是一样的吗
                oldPos++;
                newPos++;
            } else if (cmp < 0) {
                // 旧报告有一个块，而新报告没有
                removeStoredBlock(oldReport[oldPos], node);
                oldPos++;
            } else {
                // 新报告有一个旧报告没有的块
                addStoredBlock(newReport[newPos], node);
                newPos++;
            }
        }
        while (oldReport != null && oldPos < oldReport.length) {
            // 旧报告有一个块，而新报告没有
            removeStoredBlock(oldReport[oldPos], node);
            oldPos++;
        }
        while (newReport != null && newPos < newReport.length) {
            // 新报告有一个旧报告没有的块
            addStoredBlock(newReport[newPos], node);
            newPos++;
        }
        // 修改节点，使其具有新的blockreport
        node.updateBlocks(newReport);
        //我们现在已经完全更新了节点的块报告概要文件。
        //现在我们遍历它的所有块，找出哪些是无效的、不再挂起的或过度复制的。
        //(注意，仅仅在租约期满时使房屋失效是不够的;数据阳极可以在客户端对失败文件的租约到期之前失效，从而错过“过期”事件。)
        //这个函数考虑datanode上的每个块，因此只应该很少调用。
        Vector<Block> obsolete = new Vector<Block>();
        for (Iterator<Block> it = node.getBlockIterator(); it.hasNext(); ) {
            Block b = it.next();
            if (!dir.isValidBlock(b) && !pendingCreateBlocks.contains(b)) {
                LOGGER.info("Obsoleting block " + b);
                obsolete.add(b);
            }
        }
        return obsolete.toArray(new Block[obsolete.size()]);
    }

    /**
     * 修改(块- - > datanode)地图。<br/>
     * 如果解决了这个问题，则从所需的复制集中删除块。
     */
    synchronized void addStoredBlock(Block block, DatanodeInfo node) {
        TreeSet<DatanodeInfo> containingNodes = blocksMap.get(block);
        if (containingNodes == null) {
            containingNodes = new TreeSet<DatanodeInfo>();
            blocksMap.put(block, containingNodes);
        }
        if (!containingNodes.contains(node)) {
            containingNodes.add(node);
        } else {
            LOGGER.info("Redundant addStoredBlock request received for block " + block + " on node " + node);
        }
        synchronized (neededReplications) {
            if (dir.isValidBlock(block)) {
                if (containingNodes.size() >= this.desiredReplication) {
                    neededReplications.remove(block);
                    pendingReplications.remove(block);
                } else if (containingNodes.size() < this.desiredReplication) {
                    if (!neededReplications.contains(block)) {
                        neededReplications.add(block);
                    }
                }
                //查找包含多少节点是“额外的”(如果有的话)。
                //如果有任何额外的功能，调用chooseexcessrepl()来在excessReplicateMap中标记它们。
                Vector<DatanodeInfo> nonExcess = new Vector<DatanodeInfo>();
                for (Iterator<DatanodeInfo> it = containingNodes.iterator(); it.hasNext(); ) {
                    DatanodeInfo cur = it.next();
                    TreeSet<Block> excessBlocks = excessReplicateMap.get(cur.getName());
                    if (excessBlocks == null || !excessBlocks.contains(block)) {
                        nonExcess.add(cur);
                    }
                }
                if (nonExcess.size() > this.maxReplication) {
                    chooseExcessReplicates(nonExcess, block, this.maxReplication);
                }
            }
        }
    }

    /**
     * 我们希望为任何块复制最多的“maxReps”，但是现在我们有太多了。<br/>
     * 在该方法中，将足够多的节点从“srcNodes”复制到“dstNodes”中，使:
     * <p>
     * srcNodes.size() - dstNodes.size() == maxReps
     * <p>
     * 现在，我们随机选择节点。<br/>
     * 在将来，我们可能会强制执行某种策略(比如确保复制分布在机架上)。
     */
    void chooseExcessReplicates(Vector<DatanodeInfo> nonExcess, Block b, int maxReps) {
        while (nonExcess.size() - maxReps > 0) {
            int chosenNode = r.nextInt(nonExcess.size());
            DatanodeInfo cur = nonExcess.elementAt(chosenNode);
            nonExcess.removeElementAt(chosenNode);
            TreeSet<Block> excessBlocks = excessReplicateMap.get(cur.getName());
            if (excessBlocks == null) {
                excessBlocks = new TreeSet();
                excessReplicateMap.put(cur.getName(), excessBlocks);
            }
            excessBlocks.add(b);
            //“excessblocks”跟踪block，直到我们确认datanode删除了它们;我们删除它们的唯一方法是当我们收到一条“removeBlock”消息时。
            //“invalidate”列表用于通知datanode块应该被删除。
            //向namenode发出指令后，从失效列表中删除项。
            Vector<Block> invalidateSet = recentInvalidateSets.get(cur.getName());
            if (invalidateSet == null) {
                invalidateSet = new Vector<Block>();
                recentInvalidateSets.put(cur.getName(), invalidateSet);
            }
            invalidateSet.add(b);
        }
    }

    /**
     * 修改(块- - > datanode)地图。<br/>
     * 如果删除的块仍然有效，则可能生成复制任务。
     */
    synchronized void removeStoredBlock(Block block, DatanodeInfo node) {
        TreeSet<DatanodeInfo> containingNodes = blocksMap.get(block);
        if (containingNodes == null || !containingNodes.contains(node)) {
            throw new IllegalArgumentException("No machine mapping found for block " + block + ", which should be at node " + node);
        }
        containingNodes.remove(node);
        //可能是由于datanode故障而删除了该块。
        //如果该块仍然有效，检查是否需要复制。
        //在这种情况下，将block放在一个可能被复制的列表上。
        if (dir.isValidBlock(block) && (containingNodes.size() < this.desiredReplication)) {
            synchronized (neededReplications) {
                neededReplications.add(block);
            }
        }
        // 我们已经从节点中删除了一个块，所以它肯定不再是“多余的”。
        TreeSet<Block> excessBlocks = excessReplicateMap.get(node.getName());
        if (excessBlocks != null) {
            excessBlocks.remove(block);
            if (excessBlocks.size() == 0) {
                excessReplicateMap.remove(node.getName());
            }
        }
    }

    /**
     * 给定节点报告它收到了某个块。
     */
    public synchronized void blockReceived(Block block, UTF8 name) {
        DatanodeInfo node = datanodeMap.get(name);
        if (node == null) {
            throw new IllegalArgumentException("Unexpected exception.  Got blockReceived message from node " + name + ", but there is no info for " + name);
        }
        // 修改块->datanode映射
        addStoredBlock(block, node);
        // 补充节点的blockreport
        node.addBlock(block);
    }

    /**
     * 原始字节总数
     */
    public long totalCapacity() {
        return totalCapacity;
    }

    /**
     * 总未使用的原始字节
     */
    public long totalRemaining() {
        return totalRemaining;
    }

    public DatanodeInfo[] datanodeReport() {
        DatanodeInfo[] results = null;
        synchronized (heartbeats) {
            synchronized (datanodeMap) {
                results = new DatanodeInfo[datanodeMap.size()];
                int i = 0;
                for (Iterator<DatanodeInfo> it = datanodeMap.values().iterator(); it.hasNext(); ) {
                    DatanodeInfo cur = it.next();
                    results[i++] = cur;
                }
            }
        }
        return results;
    }

    /////////////////////////////////////////////////////////
    //
    // 这些方法由Namenode系统调用，以查看是否有针对给定datanode的工作。
    //
    /////////////////////////////////////////////////////////

    /**
     * 检查datanode是否应该删除最近删除的任何块。
     */
    public synchronized Block[] blocksToInvalidate(UTF8 sender) {
        Vector<Block> invalidateSet = recentInvalidateSets.remove(sender);
        if (invalidateSet != null) {
            return invalidateSet.toArray(new Block[invalidateSet.size()]);
        } else {
            return null;
        }
    }

    /**
     * 返回一个块/DataNodeInfo集合列表，指示应该在哪里复制不同的块，越快越好。<br/>
     * 我们返回的数组由两个对象组成:<br/>
     * 第一个elt是一个块数组。<br/>
     * 第二个elt是DatanodeInfo objs的2D数组，它在适当的索引处标识块的目标序列。
     */
    public synchronized Object[] pendingTransfers(DatanodeInfo srcNode, int xmitsInProgress) {
        synchronized (neededReplications) {
            Object[] results = null;
            int scheduledXfers = 0;

            if (neededReplications.size() > 0) {
                //遍历所有需要复制的块。
                //查看当前节点上是否存在。
                //如果是，请节点复制它们。
                Vector<Block> replicateBlocks = new Vector<Block>();
                Vector<DatanodeInfo[]> replicateTargetSets = new Vector<DatanodeInfo[]>();
                for (Iterator<Block> it = neededReplications.iterator(); it.hasNext(); ) {
                    // 我们只能回复“maxXfers”或更少的块
                    if (scheduledXfers >= this.maxReplicationStreams - xmitsInProgress) {
                        break;
                    }
                    Block block = it.next();
                    if (!dir.isValidBlock(block)) {
                        it.remove();
                    } else {
                        TreeSet<DatanodeInfo> containingNodes = blocksMap.get(block);
                        if (containingNodes.contains(srcNode)) {
                            DatanodeInfo[] targets = chooseTargets(Math.min(this.desiredReplication - containingNodes.size(), this.maxReplicationStreams - xmitsInProgress), containingNodes, null);
                            if (targets.length > 0) {
                                // 构建要返回的项
                                replicateBlocks.add(block);
                                replicateTargetSets.add(targets);
                                scheduledXfers += targets.length;
                            }
                        }
                    }
                }
                //将块复制移动到“挂起”状态。
                //我们使用“pending”的原因是，我们可以在适当的时间之后重试失败的复制。
                //(提醒- mjc -此计时器尚未实现。)
                if (replicateBlocks.size() > 0) {
                    int i = 0;
                    for (Iterator<Block> it = replicateBlocks.iterator(); it.hasNext(); i++) {
                        Block block = it.next();
                        DatanodeInfo[] targets = replicateTargetSets.elementAt(i);
                        TreeSet<DatanodeInfo> containingNodes = blocksMap.get(block);
                        if (containingNodes.size() + targets.length >= this.desiredReplication) {
                            neededReplications.remove(block);
                            pendingReplications.add(block);
                        }
                        LOGGER.info("Pending transfer (block " + block.getBlockName() + ") from " + srcNode.getName() + " to " + targets.length + " destinations");
                    }
                    // 从上面的列表构建返回的对象
                    DatanodeInfo[][] targetMatrix = new DatanodeInfo[replicateTargetSets.size()][];
                    for (i = 0; i < targetMatrix.length; i++) {
                        targetMatrix[i] = replicateTargetSets.elementAt(i);
                    }
                    results = new Object[2];
                    results[0] = replicateBlocks.toArray(new Block[replicateBlocks.size()]);
                    results[1] = targetMatrix;
                }
            }
            return results;
        }
    }

    /**
     * 如果可能的话，设定一定数量的目标。<br/>
     * 如果没有，尽可能多的返回。
     * @param desiredReplicates 需要复制的数量。
     * @param forbiddenNodes    不应被视为目标的DatanodeInfo实例。
     * @return DatanodeInfo实例数组用作目标。
     */
    DatanodeInfo[] chooseTargets(int desiredReplicates, TreeSet<DatanodeInfo> forbiddenNodes, UTF8 clientMachine) {
        TreeSet<DatanodeInfo> alreadyChosen = new TreeSet<DatanodeInfo>();
        Vector<DatanodeInfo> targets = new Vector<DatanodeInfo>();
        for (int i = 0; i < desiredReplicates; i++) {
            DatanodeInfo target = chooseTarget(forbiddenNodes, alreadyChosen, clientMachine);
            if (target != null) {
                targets.add(target);
                alreadyChosen.add(target);
            } else {
                // 再次调用chooseTarget没有帮助
                break;
            }
        }
        return targets.toArray(new DatanodeInfo[targets.size()]);
    }

    /**
     * 从可用的机器中选择一个目标，给定的机器除外。<br/>
     * 现在它从可用的盒子中随机选择。<br/>
     * 将来可以根据容量和负载平衡需求(甚至网络拓扑结构，以避免交换流量)进行选择。
     * @param forbidden1 不允许DatanodeInfo目标为空。
     * @param forbidden2 不允许DatanodeInfo目标为空。
     * @return 如果出错，则使用DatanodeInfo实例;如果返回null，则发出日志消息。
     */
    DatanodeInfo chooseTarget(TreeSet<DatanodeInfo> forbidden1, TreeSet<DatanodeInfo> forbidden2, UTF8 clientMachine) {
        // 检查是否有任何可用的目标
        int totalMachines = datanodeMap.size();
        if (totalMachines == 0) {
            LOGGER.warn("While choosing target, totalMachines is " + totalMachines);
            return null;
        }
        // 从两个禁用集构建禁用主机名的映射。
        TreeSet<UTF8> forbiddenMachines = new TreeSet<UTF8>();
        if (forbidden1 != null) {
            for (Iterator<DatanodeInfo> it = forbidden1.iterator(); it.hasNext(); ) {
                DatanodeInfo cur = it.next();
                forbiddenMachines.add(cur.getHost());
            }
        }
        if (forbidden2 != null) {
            for (Iterator<DatanodeInfo> it = forbidden2.iterator(); it.hasNext(); ) {
                DatanodeInfo cur = it.next();
                forbiddenMachines.add(cur.getHost());
            }
        }
        // 构建我们可以实际选择的机器列表
        Vector<DatanodeInfo> targetList = new Vector<DatanodeInfo>();
        for (Iterator<DatanodeInfo> it = datanodeMap.values().iterator(); it.hasNext(); ) {
            DatanodeInfo node = it.next();
            if (!forbiddenMachines.contains(node.getHost())) {
                targetList.add(node);
            }
        }
        Collections.shuffle(targetList);
        // 现在选择一个
        if (targetList.size() > 0) {
            // 如果请求者的机器在targetList中，并且它有容量，那么选择它。
            if (clientMachine != null && clientMachine.getLength() > 0) {
                for (Iterator<DatanodeInfo> it = targetList.iterator(); it.hasNext(); ) {
                    DatanodeInfo node = it.next();
                    if (clientMachine.equals(node.getHost())) {
                        if (node.getRemaining() > BLOCK_SIZE * MIN_BLOCKS_FOR_WRITE) {
                            return node;
                        }
                    }
                }
            }
            // 否则，根据目标容量选择节点
            for (Iterator<DatanodeInfo> it = targetList.iterator(); it.hasNext(); ) {
                DatanodeInfo node = it.next();
                if (node.getRemaining() > BLOCK_SIZE * MIN_BLOCKS_FOR_WRITE) {
                    return node;
                }
            }
            // 这样就行了。
            // 但是如果目标没有字节，我们可能无法选择任何节点。
            // 最后，选择我们能找到的第一个有效的。
            for (Iterator<DatanodeInfo> it = targetList.iterator(); it.hasNext(); ) {
                DatanodeInfo node = it.next();
                if (node.getRemaining() > BLOCK_SIZE) {
                    return node;
                }
            }
            LOGGER.warn("Could not find any nodes with sufficient capacity");
            return null;
        } else {
            LOGGER.warn("Zero targets found, forbidden1.size=" +
                    (forbidden1 != null ? forbidden1.size() : 0) +
                    " forbidden2.size()=" +
                    (forbidden2 != null ? forbidden2.size() : 0));
            return null;
        }
    }
}
