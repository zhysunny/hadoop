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

import org.apache.hadoop.io.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.util.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
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
        this.desiredReplication = conf.getInt(ConfigConstants.DFS_REPLICATION, ConfigConstants.DFS_REPLICATION_DEFAULT);
        this.maxReplication = desiredReplication;
        this.maxReplicationStreams = conf.getInt(ConfigConstants.DFS_MAX_REPL_STREAMS, ConfigConstants.DFS_MAX_REPL_STREAMS_DEFAULT);
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
            // using finally to ensure we also wait for lease daemon
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
     * The client wants to open the given filename.  Return a
     * list of (block,machineArray) pairs.  The sequence of unique blocks
     * in the list indicates all the blocks that make up the filename.
     * <p>
     * The client should choose one of the machines from the machineArray
     * at random.
     */
    public Object[] open(UTF8 src) {
        Object results[] = null;
        Block blocks[] = dir.getFile(src);
        if (blocks != null) {
            results = new Object[2];
            DatanodeInfo machineSets[][] = new DatanodeInfo[blocks.length][];

            for (int i = 0; i < blocks.length; i++) {
                TreeSet containingNodes = (TreeSet) blocksMap.get(blocks[i]);
                if (containingNodes == null) {
                    machineSets[i] = new DatanodeInfo[0];
                } else {
                    machineSets[i] = new DatanodeInfo[containingNodes.size()];
                    int j = 0;
                    for (Iterator it = containingNodes.iterator(); it.hasNext(); j++) {
                        machineSets[i][j] = (DatanodeInfo) it.next();
                    }
                }
            }

            results[0] = blocks;
            results[1] = machineSets;
        }
        return results;
    }

    /**
     * The client would like to create a new block for the indicated
     * filename.  Return an array that consists of the block, plus a set
     * of machines.  The first on this list should be where the client
     * writes data.  Subsequent items in the list must be provided in
     * the connection to the first datanode.
     * @return Return an array that consists of the block, plus a set
     * of machines, or null if src is invalid for creation (based on
     * {@link FSDirectory#isValidToCreate(UTF8)}.
     */
    public synchronized Object[] startFile(UTF8 src, UTF8 holder, UTF8 clientMachine, boolean overwrite) {
        Object results[] = null;
        if (pendingCreates.get(src) == null) {
            boolean fileValid = dir.isValidToCreate(src);
            if (overwrite && !fileValid) {
                delete(src);
                fileValid = true;
            }

            if (fileValid) {
                results = new Object[2];

                // Get the array of replication targets 
                DatanodeInfo targets[] = chooseTargets(this.desiredReplication, null, clientMachine);
                if (targets.length < this.minReplication) {
                    LOGGER.warn("Target-length is " + targets.length +
                            ", below MIN_REPLICATION (" + this.minReplication + ")");
                    return null;
                }

                // Reserve space for this pending file
                pendingCreates.put(src, new Vector());
                synchronized (leases) {
                    Lease lease = (Lease) leases.get(holder);
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

                // Create next block
                results[0] = allocateBlock(src);
                results[1] = targets;
            } else { // ! fileValid
                LOGGER.warn("Cannot start file because it is invalid. src=" + src);
            }
        } else {
            LOGGER.warn("Cannot start file because pendingCreates is non-null. src=" + src);
        }
        return results;
    }

    /**
     * The client would like to obtain an additional block for the indicated
     * filename (which is being written-to).  Return an array that consists
     * of the block, plus a set of machines.  The first on this list should
     * be where the client writes data.  Subsequent items in the list must
     * be provided in the connection to the first datanode.
     * <p>
     * Make sure the previous blocks have been reported by datanodes and
     * are replicated.  Will return an empty 2-elt array if we want the
     * client to "try again later".
     */
    public synchronized Object[] getAdditionalBlock(UTF8 src, UTF8 clientMachine) {
        Object results[] = null;
        if (dir.getFile(src) == null && pendingCreates.get(src) != null) {
            results = new Object[2];

            //
            // If we fail this, bad things happen!
            //
            if (checkFileProgress(src)) {
                // Get the array of replication targets 
                DatanodeInfo targets[] = chooseTargets(this.desiredReplication, null, clientMachine);
                if (targets.length < this.minReplication) {
                    return null;
                }

                // Create next block
                results[0] = allocateBlock(src);
                results[1] = targets;
            }
        }
        return results;
    }

    /**
     * The client would like to let go of the given block
     */
    public synchronized boolean abandonBlock(Block b, UTF8 src) {
        //
        // Remove the block from the pending creates list
        //
        Vector pendingVector = (Vector) pendingCreates.get(src);
        if (pendingVector != null) {
            for (Iterator it = pendingVector.iterator(); it.hasNext(); ) {
                Block cur = (Block) it.next();
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
     * Abandon the entire file in progress
     */
    public synchronized void abandonFileInProgress(UTF8 src) throws IOException {
        internalReleaseCreate(src);
    }

    /**
     * Finalize the created file and make it world-accessible.  The
     * FSNamesystem will already know the blocks that make up the file.
     * Before we return, we make sure that all the file's blocks have
     * been reported by datanodes and are replicated correctly.
     */
    public synchronized int completeFile(UTF8 src, UTF8 holder) {
        if (dir.getFile(src) != null || pendingCreates.get(src) == null) {
            LOGGER.info("Failed to complete " + src + "  because dir.getFile()==" + dir.getFile(src) + " and " + pendingCreates.get(src));
            return OPERATION_FAILED;
        } else if (!checkFileProgress(src)) {
            return STILL_WAITING;
        } else {
            Vector pendingVector = (Vector) pendingCreates.get(src);
            Block pendingBlocks[] = (Block[]) pendingVector.toArray(new Block[pendingVector.size()]);

            //
            // We have the pending blocks, but they won't have
            // length info in them (as they were allocated before
            // data-write took place).  So we need to add the correct
            // length info to each
            //
            // REMIND - mjc - this is very inefficient!  We should
            // improve this!
            //
            for (int i = 0; i < pendingBlocks.length; i++) {
                Block b = pendingBlocks[i];
                TreeSet containingNodes = (TreeSet) blocksMap.get(b);
                DatanodeInfo node = (DatanodeInfo) containingNodes.first();
                for (Iterator it = node.getBlockIterator(); it.hasNext(); ) {
                    Block cur = (Block) it.next();
                    if (b.getBlockId() == cur.getBlockId()) {
                        b.setNumBytes(cur.getNumBytes());
                        break;
                    }
                }
            }

            //
            // Now we can add the (name,blocks) tuple to the filesystem
            //
            if (dir.addFile(src, pendingBlocks)) {
                // The file is no longer pending
                pendingCreates.remove(src);
                for (int i = 0; i < pendingBlocks.length; i++) {
                    pendingCreateBlocks.remove(pendingBlocks[i]);
                }

                synchronized (leases) {
                    Lease lease = (Lease) leases.get(holder);
                    if (lease != null) {
                        lease.completedCreate(src);
                        if (!lease.hasLocks()) {
                            leases.remove(holder);
                            sortedLeases.remove(lease);
                        }
                    }
                }

                //
                // REMIND - mjc - this should be done only after we wait a few secs.
                // The namenode isn't giving datanodes enough time to report the
                // replicated blocks that are automatically done as part of a client
                // write.
                //

                // Now that the file is real, we need to be sure to replicate
                // the blocks.
                for (int i = 0; i < pendingBlocks.length; i++) {
                    TreeSet containingNodes = (TreeSet) blocksMap.get(pendingBlocks[i]);
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
     * Allocate a block at the given pending filename
     */
    synchronized Block allocateBlock(UTF8 src) {
        Block b = new Block();
        Vector v = (Vector) pendingCreates.get(src);
        v.add(b);
        pendingCreateBlocks.add(b);
        return b;
    }

    /**
     * Check that the indicated file's blocks are present and
     * replicated.  If not, return false.
     */
    synchronized boolean checkFileProgress(UTF8 src) {
        Vector v = (Vector) pendingCreates.get(src);

        for (Iterator it = v.iterator(); it.hasNext(); ) {
            Block b = (Block) it.next();
            TreeSet containingNodes = (TreeSet) blocksMap.get(b);
            if (containingNodes == null || containingNodes.size() < this.minReplication) {
                return false;
            }
        }
        return true;
    }

    ////////////////////////////////////////////////////////////////
    // Here's how to handle block-copy failure during client write:
    // -- As usual, the client's write should result in a streaming
    // backup write to a k-machine sequence.
    // -- If one of the backup machines fails, no worries.  Fail silently.
    // -- Before client is allowed to close and finalize file, make sure
    // that the blocks are backed up.  Namenode may have to issue specific backup
    // commands to make up for earlier datanode failures.  Once all copies
    // are made, edit namespace and return to client.
    ////////////////////////////////////////////////////////////////

    /**
     * Change the indicated filename.
     */
    public boolean renameTo(UTF8 src, UTF8 dst) {
        return dir.renameTo(src, dst);
    }

    /**
     * Remove the indicated filename from the namespace.  This may
     * invalidate some blocks that make up the file.
     */
    public synchronized boolean delete(UTF8 src) {
        Block deletedBlocks[] = (Block[]) dir.delete(src);
        if (deletedBlocks != null) {
            for (int i = 0; i < deletedBlocks.length; i++) {
                Block b = deletedBlocks[i];

                TreeSet containingNodes = (TreeSet) blocksMap.get(b);
                if (containingNodes != null) {
                    for (Iterator it = containingNodes.iterator(); it.hasNext(); ) {
                        DatanodeInfo node = (DatanodeInfo) it.next();
                        Vector invalidateSet = (Vector) recentInvalidateSets.get(node.getName());
                        if (invalidateSet == null) {
                            invalidateSet = new Vector();
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
     * Return whether the given filename exists
     */
    public boolean exists(UTF8 src) {
        if (dir.getFile(src) != null || dir.isDir(src)) {
            return true;
        } else {
            return false;
        }
    }

    /**
     * Whether the given name is a directory
     */
    public boolean isDir(UTF8 src) {
        return dir.isDir(src);
    }

    /**
     * Create all the necessary directories
     */
    public boolean mkdirs(UTF8 src) {
        return dir.mkdirs(src);
    }

    /**
     * Figure out a few hosts that are likely to contain the
     * block(s) referred to by the given (filename, start, len) tuple.
     */
    public UTF8[][] getDatanodeHints(UTF8 src, long start, long len) {
        if (start < 0 || len < 0) {
            return new UTF8[0][];
        }

        int startBlock = -1;
        int endBlock = -1;
        Block blocks[] = dir.getFile(src);

        if (blocks == null) {                     // no blocks
            return new UTF8[0][];
        }

        //
        // First, figure out where the range falls in
        // the blocklist.
        //
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

        //
        // Next, create an array of hosts where each block can
        // be found
        //
        if (startBlock < 0 || endBlock < 0) {
            return new UTF8[0][];
        } else {
            UTF8 hosts[][] = new UTF8[(endBlock - startBlock) + 1][];
            for (int i = startBlock; i <= endBlock; i++) {
                TreeSet containingNodes = (TreeSet) blocksMap.get(blocks[i]);
                Vector v = new Vector();
                for (Iterator it = containingNodes.iterator(); it.hasNext(); ) {
                    DatanodeInfo cur = (DatanodeInfo) it.next();
                    v.add(cur.getHost());
                }
                hosts[i - startBlock] = (UTF8[]) v.toArray(new UTF8[v.size()]);
            }
            return hosts;
        }
    }

    /************************************************************
     * A Lease governs all the locks held by a single client.
     * For each client there's a corresponding lease, whose
     * timestamp is updated when the client periodically
     * checks in.  If the client dies and allows its lease to
     * expire, all the corresponding locks can be released.
     *************************************************************/
    class Lease implements Comparable {
        public UTF8 holder;
        public long lastUpdate;
        TreeSet locks = new TreeSet();
        TreeSet creates = new TreeSet();

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

        /**
         */
        @Override
        public String toString() {
            return "[Lease.  Holder: " + holder.toString() + ", heldlocks: " + locks.size() + ", pendingcreates: " + creates.size() + "]";
        }

        /**
         */
        @Override
        public int compareTo(Object o) {
            Lease l1 = (Lease) this;
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
    }

    /******************************************************
     * LeaseMonitor checks for leases that have expired,
     * and disposes of them.
     ******************************************************/
    class LeaseMonitor implements Runnable {
        @Override
        public void run() {
            while (fsRunning) {
                synchronized (FSNamesystem.this) {
                    synchronized (leases) {
                        Lease top;
                        while ((sortedLeases.size() > 0) &&
                                ((top = (Lease) sortedLeases.first()) != null)) {
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
     * Get a lock (perhaps exclusive) on the given file
     */
    public synchronized int obtainLock(UTF8 src, UTF8 holder, boolean exclusive) {
        int result = dir.obtainLock(src, holder, exclusive);
        if (result == COMPLETE_SUCCESS) {
            synchronized (leases) {
                Lease lease = (Lease) leases.get(holder);
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
     * Release the lock on the given file
     */
    public synchronized int releaseLock(UTF8 src, UTF8 holder) {
        int result = internalReleaseLock(src, holder);
        if (result == COMPLETE_SUCCESS) {
            synchronized (leases) {
                Lease lease = (Lease) leases.get(holder);
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
        Vector v = (Vector) pendingCreates.remove(src);
        for (Iterator it2 = v.iterator(); it2.hasNext(); ) {
            Block b = (Block) it2.next();
            pendingCreateBlocks.remove(b);
        }
    }

    /**
     * Renew the lease(s) held by the given client
     */
    public void renewLease(UTF8 holder) {
        synchronized (leases) {
            Lease lease = (Lease) leases.get(holder);
            if (lease != null) {
                sortedLeases.remove(lease);
                lease.renew();
                sortedLeases.add(lease);
            }
        }
    }

    /**
     * Get a listing of all files at 'src'.  The Object[] array
     * exists so we can return file attributes (soon to be implemented)
     */
    public DFSFileInfo[] getListing(UTF8 src) {
        return dir.getListing(src);
    }

    /////////////////////////////////////////////////////////
    //
    // These methods are called by datanodes
    //
    /////////////////////////////////////////////////////////

    /**
     * The given node has reported in.  This method should:
     * 1) Record the heartbeat, so the datanode isn't timed out
     * 2) Adjust usage stats for future block allocation
     */
    public synchronized void gotHeartbeat(UTF8 name, long capacity, long remaining) {
        synchronized (heartbeats) {
            synchronized (datanodeMap) {
                long capacityDiff = 0;
                long remainingDiff = 0;
                DatanodeInfo nodeinfo = (DatanodeInfo) datanodeMap.get(name);

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
     * Periodically calls heartbeatCheck().
     */
    class HeartbeatMonitor implements Runnable {
        /**
         */
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
     * Check if there are any expired heartbeats, and if so,
     * whether any blocks have to be re-replicated.
     */
    synchronized void heartbeatCheck() {
        synchronized (heartbeats) {
            DatanodeInfo nodeInfo = null;

            while ((heartbeats.size() > 0) &&
                    ((nodeInfo = (DatanodeInfo) heartbeats.first()) != null) &&
                    (nodeInfo.lastUpdate() < System.currentTimeMillis() - EXPIRE_INTERVAL)) {
                LOGGER.info("Lost heartbeat for " + nodeInfo.getName());

                heartbeats.remove(nodeInfo);
                synchronized (datanodeMap) {
                    datanodeMap.remove(nodeInfo.getName());
                }
                totalCapacity -= nodeInfo.getCapacity();
                totalRemaining -= nodeInfo.getRemaining();

                Block deadblocks[] = nodeInfo.getBlocks();
                if (deadblocks != null) {
                    for (int i = 0; i < deadblocks.length; i++) {
                        removeStoredBlock(deadblocks[i], nodeInfo);
                    }
                }

                if (heartbeats.size() > 0) {
                    nodeInfo = (DatanodeInfo) heartbeats.first();
                }
            }
        }
    }

    /**
     * The given node is reporting all its blocks.  Use this info to
     * update the (machine-->blocklist) and (block-->machinelist) tables.
     */
    public synchronized Block[] processReport(Block newReport[], UTF8 name) {
        DatanodeInfo node = (DatanodeInfo) datanodeMap.get(name);
        if (node == null) {
            throw new IllegalArgumentException("Unexpected exception.  Received block report from node " + name + ", but there is no info for " + name);
        }

        //
        // Modify the (block-->datanode) map, according to the difference
        // between the old and new block report.
        //
        int oldPos = 0, newPos = 0;
        Block oldReport[] = node.getBlocks();
        while (oldReport != null && newReport != null && oldPos < oldReport.length && newPos < newReport.length) {
            int cmp = oldReport[oldPos].compareTo(newReport[newPos]);

            if (cmp == 0) {
                // Do nothing, blocks are the same
                oldPos++;
                newPos++;
            } else if (cmp < 0) {
                // The old report has a block the new one does not
                removeStoredBlock(oldReport[oldPos], node);
                oldPos++;
            } else {
                // The new report has a block the old one does not
                addStoredBlock(newReport[newPos], node);
                newPos++;
            }
        }
        while (oldReport != null && oldPos < oldReport.length) {
            // The old report has a block the new one does not
            removeStoredBlock(oldReport[oldPos], node);
            oldPos++;
        }
        while (newReport != null && newPos < newReport.length) {
            // The new report has a block the old one does not
            addStoredBlock(newReport[newPos], node);
            newPos++;
        }

        //
        // Modify node so it has the new blockreport
        //
        node.updateBlocks(newReport);

        //
        // We've now completely updated the node's block report profile.
        // We now go through all its blocks and find which ones are invalid,
        // no longer pending, or over-replicated.
        //
        // (Note it's not enough to just invalidate blocks at lease expiry 
        // time; datanodes can go down before the client's lease on 
        // the failed file expires and miss the "expire" event.)
        //
        // This function considers every block on a datanode, and thus
        // should only be invoked infrequently.
        //
        Vector obsolete = new Vector();
        for (Iterator it = node.getBlockIterator(); it.hasNext(); ) {
            Block b = (Block) it.next();

            if (!dir.isValidBlock(b) && !pendingCreateBlocks.contains(b)) {
                LOGGER.info("Obsoleting block " + b);
                obsolete.add(b);
            }
        }
        return (Block[]) obsolete.toArray(new Block[obsolete.size()]);
    }

    /**
     * Modify (block-->datanode) map.  Remove block from set of
     * needed replications if this takes care of the problem.
     */
    synchronized void addStoredBlock(Block block, DatanodeInfo node) {
        TreeSet containingNodes = (TreeSet) blocksMap.get(block);
        if (containingNodes == null) {
            containingNodes = new TreeSet();
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

                //
                // Find how many of the containing nodes are "extra", if any.
                // If there are any extras, call chooseExcessReplicates() to
                // mark them in the excessReplicateMap.
                //
                Vector nonExcess = new Vector();
                for (Iterator it = containingNodes.iterator(); it.hasNext(); ) {
                    DatanodeInfo cur = (DatanodeInfo) it.next();
                    TreeSet excessBlocks = (TreeSet) excessReplicateMap.get(cur.getName());
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
     * We want a max of "maxReps" replicates for any block, but we now have too many.
     * In this method, copy enough nodes from 'srcNodes' into 'dstNodes' such that:
     * <p>
     * srcNodes.size() - dstNodes.size() == maxReps
     * <p>
     * For now, we choose nodes randomly.  In the future, we might enforce some
     * kind of policy (like making sure replicates are spread across racks).
     */
    void chooseExcessReplicates(Vector nonExcess, Block b, int maxReps) {
        while (nonExcess.size() - maxReps > 0) {
            int chosenNode = r.nextInt(nonExcess.size());
            DatanodeInfo cur = (DatanodeInfo) nonExcess.elementAt(chosenNode);
            nonExcess.removeElementAt(chosenNode);

            TreeSet excessBlocks = (TreeSet) excessReplicateMap.get(cur.getName());
            if (excessBlocks == null) {
                excessBlocks = new TreeSet();
                excessReplicateMap.put(cur.getName(), excessBlocks);
            }
            excessBlocks.add(b);

            //
            // The 'excessblocks' tracks blocks until we get confirmation
            // that the datanode has deleted them; the only way we remove them
            // is when we get a "removeBlock" message.  
            //
            // The 'invalidate' list is used to inform the datanode the block 
            // should be deleted.  Items are removed from the invalidate list
            // upon giving instructions to the namenode.
            //
            Vector invalidateSet = (Vector) recentInvalidateSets.get(cur.getName());
            if (invalidateSet == null) {
                invalidateSet = new Vector();
                recentInvalidateSets.put(cur.getName(), invalidateSet);
            }
            invalidateSet.add(b);
        }
    }

    /**
     * Modify (block-->datanode) map.  Possibly generate
     * replication tasks, if the removed block is still valid.
     */
    synchronized void removeStoredBlock(Block block, DatanodeInfo node) {
        TreeSet containingNodes = (TreeSet) blocksMap.get(block);
        if (containingNodes == null || !containingNodes.contains(node)) {
            throw new IllegalArgumentException("No machine mapping found for block " + block + ", which should be at node " + node);
        }
        containingNodes.remove(node);

        //
        // It's possible that the block was removed because of a datanode
        // failure.  If the block is still valid, check if replication is
        // necessary.  In that case, put block on a possibly-will-
        // be-replicated list.
        //
        if (dir.isValidBlock(block) && (containingNodes.size() < this.desiredReplication)) {
            synchronized (neededReplications) {
                neededReplications.add(block);
            }
        }

        //
        // We've removed a block from a node, so it's definitely no longer
        // in "excess" there.
        //
        TreeSet excessBlocks = (TreeSet) excessReplicateMap.get(node.getName());
        if (excessBlocks != null) {
            excessBlocks.remove(block);
            if (excessBlocks.size() == 0) {
                excessReplicateMap.remove(node.getName());
            }
        }
    }

    /**
     * The given node is reporting that it received a certain block.
     */
    public synchronized void blockReceived(Block block, UTF8 name) {
        DatanodeInfo node = (DatanodeInfo) datanodeMap.get(name);
        if (node == null) {
            throw new IllegalArgumentException("Unexpected exception.  Got blockReceived message from node " + name + ", but there is no info for " + name);
        }
        //
        // Modify the blocks->datanode map
        // 
        addStoredBlock(block, node);

        //
        // Supplement node's blockreport
        //
        node.addBlock(block);
    }

    /**
     * Total raw bytes
     */
    public long totalCapacity() {
        return totalCapacity;
    }

    /**
     * Total non-used raw bytes
     */
    public long totalRemaining() {
        return totalRemaining;
    }

    /**
     */
    public DatanodeInfo[] datanodeReport() {
        DatanodeInfo results[] = null;
        synchronized (heartbeats) {
            synchronized (datanodeMap) {
                results = new DatanodeInfo[datanodeMap.size()];
                int i = 0;
                for (Iterator it = datanodeMap.values().iterator(); it.hasNext(); ) {
                    DatanodeInfo cur = (DatanodeInfo) it.next();
                    results[i++] = cur;
                }
            }
        }
        return results;
    }

    /////////////////////////////////////////////////////////
    //
    // These methods are called by the Namenode system, to see
    // if there is any work for a given datanode.
    //
    /////////////////////////////////////////////////////////

    /**
     * Check if there are any recently-deleted blocks a datanode should remove.
     */
    public synchronized Block[] blocksToInvalidate(UTF8 sender) {
        Vector invalidateSet = (Vector) recentInvalidateSets.remove(sender);
        if (invalidateSet != null) {
            return (Block[]) invalidateSet.toArray(new Block[invalidateSet.size()]);
        } else {
            return null;
        }
    }

    /**
     * Return with a list of Block/DataNodeInfo sets, indicating
     * where various Blocks should be copied, ASAP.
     * <p>
     * The Array that we return consists of two objects:
     * The 1st elt is an array of Blocks.
     * The 2nd elt is a 2D array of DatanodeInfo objs, identifying the
     * target sequence for the Block at the appropriate index.
     */
    public synchronized Object[] pendingTransfers(DatanodeInfo srcNode, int xmitsInProgress) {
        synchronized (neededReplications) {
            Object results[] = null;
            int scheduledXfers = 0;

            if (neededReplications.size() > 0) {
                //
                // Go through all blocks that need replications.  See if any
                // are present at the current node.  If so, ask the node to
                // replicate them.
                //
                Vector replicateBlocks = new Vector();
                Vector replicateTargetSets = new Vector();
                for (Iterator it = neededReplications.iterator(); it.hasNext(); ) {
                    //
                    // We can only reply with 'maxXfers' or fewer blocks
                    //
                    if (scheduledXfers >= this.maxReplicationStreams - xmitsInProgress) {
                        break;
                    }

                    Block block = (Block) it.next();
                    if (!dir.isValidBlock(block)) {
                        it.remove();
                    } else {
                        TreeSet containingNodes = (TreeSet) blocksMap.get(block);
                        if (containingNodes.contains(srcNode)) {
                            DatanodeInfo targets[] = chooseTargets(Math.min(this.desiredReplication - containingNodes.size(), this.maxReplicationStreams - xmitsInProgress), containingNodes, null);
                            if (targets.length > 0) {
                                // Build items to return
                                replicateBlocks.add(block);
                                replicateTargetSets.add(targets);
                                scheduledXfers += targets.length;
                            }
                        }
                    }
                }

                //
                // Move the block-replication into a "pending" state.
                // The reason we use 'pending' is so we can retry
                // replications that fail after an appropriate amount of time.  
                // (REMIND - mjc - this timer is not yet implemented.)
                //
                if (replicateBlocks.size() > 0) {
                    int i = 0;
                    for (Iterator it = replicateBlocks.iterator(); it.hasNext(); i++) {
                        Block block = (Block) it.next();
                        DatanodeInfo targets[] = (DatanodeInfo[]) replicateTargetSets.elementAt(i);
                        TreeSet containingNodes = (TreeSet) blocksMap.get(block);

                        if (containingNodes.size() + targets.length >= this.desiredReplication) {
                            neededReplications.remove(block);
                            pendingReplications.add(block);
                        }

                        LOGGER.info("Pending transfer (block " + block.getBlockName() + ") from " + srcNode.getName() + " to " + targets.length + " destinations");
                    }

                    //
                    // Build returned objects from above lists
                    //
                    DatanodeInfo targetMatrix[][] = new DatanodeInfo[replicateTargetSets.size()][];
                    for (i = 0; i < targetMatrix.length; i++) {
                        targetMatrix[i] = (DatanodeInfo[]) replicateTargetSets.elementAt(i);
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
     * Get a certain number of targets, if possible.
     * If not, return as many as we can.
     * @param desiredReplicates number of duplicates wanted.
     * @param forbiddenNodes    of DatanodeInfo instances that should not be
     *                          considered targets.
     * @return array of DatanodeInfo instances uses as targets.
     */
    DatanodeInfo[] chooseTargets(int desiredReplicates, TreeSet forbiddenNodes, UTF8 clientMachine) {
        TreeSet alreadyChosen = new TreeSet();
        Vector targets = new Vector();

        for (int i = 0; i < desiredReplicates; i++) {
            DatanodeInfo target = chooseTarget(forbiddenNodes, alreadyChosen, clientMachine);
            if (target != null) {
                targets.add(target);
                alreadyChosen.add(target);
            } else {
                break; // calling chooseTarget again won't help
            }
        }
        return (DatanodeInfo[]) targets.toArray(new DatanodeInfo[targets.size()]);
    }

    /**
     * Choose a target from available machines, excepting the
     * given ones.
     * <p>
     * Right now it chooses randomly from available boxes.  In future could
     * choose according to capacity and load-balancing needs (or even
     * network-topology, to avoid inter-switch traffic).
     * @param forbidden1 DatanodeInfo targets not allowed, null allowed.
     * @param forbidden2 DatanodeInfo targets not allowed, null allowed.
     * @return DatanodeInfo instance to use or null if something went wrong
     * (a log message is emitted if null is returned).
     */
    DatanodeInfo chooseTarget(TreeSet forbidden1, TreeSet forbidden2, UTF8 clientMachine) {
        //
        // Check if there are any available targets at all
        //
        int totalMachines = datanodeMap.size();
        if (totalMachines == 0) {
            LOGGER.warn("While choosing target, totalMachines is " + totalMachines);
            return null;
        }

        //
        // Build a map of forbidden hostnames from the two forbidden sets.
        //
        TreeSet forbiddenMachines = new TreeSet();
        if (forbidden1 != null) {
            for (Iterator it = forbidden1.iterator(); it.hasNext(); ) {
                DatanodeInfo cur = (DatanodeInfo) it.next();
                forbiddenMachines.add(cur.getHost());
            }
        }
        if (forbidden2 != null) {
            for (Iterator it = forbidden2.iterator(); it.hasNext(); ) {
                DatanodeInfo cur = (DatanodeInfo) it.next();
                forbiddenMachines.add(cur.getHost());
            }
        }

        //
        // Build list of machines we can actually choose from
        //
        Vector targetList = new Vector();
        for (Iterator it = datanodeMap.values().iterator(); it.hasNext(); ) {
            DatanodeInfo node = (DatanodeInfo) it.next();
            if (!forbiddenMachines.contains(node.getHost())) {
                targetList.add(node);
            }
        }
        Collections.shuffle(targetList);

        //
        // Now pick one
        //
        if (targetList.size() > 0) {
            //
            // If the requester's machine is in the targetList, 
            // and it's got the capacity, pick it.
            //
            if (clientMachine != null && clientMachine.getLength() > 0) {
                for (Iterator it = targetList.iterator(); it.hasNext(); ) {
                    DatanodeInfo node = (DatanodeInfo) it.next();
                    if (clientMachine.equals(node.getHost())) {
                        if (node.getRemaining() > BLOCK_SIZE * MIN_BLOCKS_FOR_WRITE) {
                            return node;
                        }
                    }
                }
            }

            //
            // Otherwise, choose node according to target capacity
            //
            for (Iterator it = targetList.iterator(); it.hasNext(); ) {
                DatanodeInfo node = (DatanodeInfo) it.next();
                if (node.getRemaining() > BLOCK_SIZE * MIN_BLOCKS_FOR_WRITE) {
                    return node;
                }
            }

            //
            // That should do the trick.  But we might not be able
            // to pick any node if the target was out of bytes.  As
            // a last resort, pick the first valid one we can find.
            //
            for (Iterator it = targetList.iterator(); it.hasNext(); ) {
                DatanodeInfo node = (DatanodeInfo) it.next();
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
