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

/**
 * 一段DFS用户代码使用ClientProtocol与NameNode通信。用户代码可以操作目录名称空间，以及打开/关闭文件流等。
 * @author 章云
 * @date 2019/7/30 15:56
 */
public interface ClientProtocol {

    ///////////////////////////////////////
    // 文件内容
    ///////////////////////////////////////

    /**
     * 按给定名称打开现有文件。返回数据块和数据阳极信息。
     * 然后客户端必须与每个指定的DataNode联系，以获得实际数据。在调用open()之后，不需要调用close()或任何其他函数。
     * @param src
     * @return
     * @throws IOException
     */
    LocatedBlock[] open(String src) throws IOException;

    /**
     * 创建一个新文件。获取块和datanode信息，该信息描述第一个块应该写在哪里。
     * 成功地调用此方法可以防止任何其他客户机以给定的名称创建文件，但是调用者必须调用complete()才能将文件添加到文件系统中。
     * 块具有最大大小。想要创建多块文件的客户端还必须使用reportWrittenBlock()和addBlock()。
     * @param src
     * @param clientName
     * @param clientMachine
     * @param overwrite
     * @return
     * @throws IOException
     */
    LocatedBlock create(String src, String clientName, String clientMachine, boolean overwrite) throws IOException;

    /**
     * 编写了数据块的客户机可以使用reportWrittenBlock()向NameNode报告完成情况。
     * 客户端无法获得额外的块，直到前一个块被报告为已写或已被放弃。
     * @param b
     * @throws IOException
     */
    void reportWrittenBlock(LocatedBlock b) throws IOException;

    /**
     * 如果客户端还没有调用reportWrittenBlock()，它可以通过调用onblock()放弃它。
     * 然后，客户机可以获得一个新的块，或者完成或放弃文件。
     * 对块的任何部分写操作都将被垃圾收集。
     * @param b
     * @param src
     * @throws IOException
     */
    void abandonBlock(Block b, String src) throws IOException;

    /**
     * 客户端如果想要向指定的文件名写入一个额外的块(该文件名当前必须打开以便写入)，应该调用addBlock()。
     * addBlock()返回block和datanode信息，就像初始调用create()一样。
     * null响应意味着NameNode不能分配块，调用者应该再试一次。
     * @param src
     * @param clientMachine
     * @return
     * @throws IOException
     */
    LocatedBlock addBlock(String src, String clientMachine) throws IOException;

    /**
     * 想要放弃对当前文件的写入的客户端应该调用onfileinprogress()。
     * 在此调用之后，任何客户机都可以调用create()来获取文件名。
     * 为该文件编写的任何块都将被垃圾收集。
     * @param src
     * @throws IOException
     */
    void abandonFileInProgress(String src) throws IOException;

    /**
     * 客户端已完成对给定文件名的数据写入，并希望完成它。
     * 函数返回文件是否已成功关闭。
     * 如果函数返回false，调用者应该再试一次。
     * 在所有文件块被复制最少次数之前，对complete()的调用不会返回true。因此，DataNode失败可能导致客户机在成功之前多次调用complete()。
     * @param src
     * @param clientName
     * @return
     * @throws IOException
     */
    boolean complete(String src, String clientName) throws IOException;

    ///////////////////////////////////////
    // 命名空间管理
    ///////////////////////////////////////

    /**
     * 在fs名称空间中重命名项
     * @param src
     * @param dst
     * @return
     * @throws IOException
     */
    boolean rename(String src, String dst) throws IOException;

    /**
     * 从文件系统中删除给定的文件名
     * @param src
     * @return
     * @throws IOException
     */
    boolean delete(String src) throws IOException;

    /**
     * 检查给定文件是否存在
     * @param src
     * @return
     * @throws IOException
     */
    boolean exists(String src) throws IOException;

    /**
     * 检查给定的文件名是否为目录。
     * @param src
     * @return
     * @throws IOException
     */
    boolean isDir(String src) throws IOException;

    /**
     * 创建具有给定名称的目录(或目录层次结构)。
     * @param src
     * @return
     * @throws IOException
     */
    boolean mkdirs(String src) throws IOException;

    /**
     * 获取指定目录的列表
     * @param src
     * @return
     * @throws IOException
     */
    DFSFileInfo[] getListing(String src) throws IOException;

    ///////////////////////////////////////
    // 系统问题及管理
    ///////////////////////////////////////

    /**
     * getHints()返回存储特定文件区域数据的主机名列表。它为指定区域内的每个块返回一组主机名。
     * 这个函数在编写执行操作时考虑数据位置的代码时非常有用。例如，MapReduce系统尝试在与任务处理的数据块相同的机器上调度任务。
     * @param src
     * @param start
     * @param len
     * @return
     * @throws IOException
     */
    String[][] getHints(String src, long start, long len) throws IOException;

    /**
     * obtainLock()用于锁管理网络。如果锁被正确地捕获，则返回true。如果无法获得锁，则返回false，客户机应重试。
     * 锁定是大多数文件系统的一部分，对于许多进程间同步任务非常有用。
     * @param src
     * @param clientName
     * @param exclusive
     * @return
     * @throws IOException
     */
    boolean obtainLock(String src, String clientName, boolean exclusive) throws IOException;

    /**
     * 如果客户机希望释放一个持有的锁，则调用releaseLock()。如果锁被正确释放，则返回true。
     * 如果客户机应该等待并重试，则t返回false。
     * @param src
     * @param clientName
     * @return
     * @throws IOException
     */
    boolean releaseLock(String src, String clientName) throws IOException;

    /**
     * 客户端程序可以导致NameNode中的有状态更改，从而影响其他客户端。客户端可以获得一个文件，但既不放弃也不完成它。客户端可能持有一系列锁，阻止其他客户端继续。
     * 显然，如果客户端持有一堆它从未放弃的锁，这将是糟糕的。如果客户机意外死亡，这很容易发生。
     * 因此，NameNode将撤销锁并为它认为已死的客户机创建活动文件。客户机通过定期调用renewLease()告诉NameNode它仍然是活动的。如果从最后一次调用renewLease()起经过一定时间，NameNode假定客户端已经死亡。
     * @param clientName
     * @throws IOException
     */
    void renewLease(String clientName) throws IOException;

    /**
     * 获取一组关于文件系统的统计信息。
     * 现在，只返回两个值。
     * [0]包含系统的总存储容量，单位为字节。
     * [1]包含系统的可用存储(以字节为单位)。
     * @return
     * @throws IOException
     */
    long[] getStats() throws IOException;

    /**
     * 获取关于系统当前datanodes的完整报告。
     * 每个DataNode返回一个DatanodeInfo对象。
     * @return
     * @throws IOException
     */
    DatanodeInfo[] getDatanodeReport() throws IOException;
}
