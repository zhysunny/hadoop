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
 * DFS datanode用于与NameNode通信的协议。
 * 它用于上传当前加载信息和阻塞报告。
 * NameNode与DataNode通信的唯一方法是从这些函数返回值。
 * @author 章云
 * @date 2019/7/30 16:22
 */
interface DatanodeProtocol {
    /**
     * sendHeartbeat()告诉NameNode, DataNode仍然是活动的，并且运行良好。还包括一些状态信息。
     * @param sender
     * @param capacity
     * @param remaining
     * @throws IOException
     */
    void sendHeartbeat(String sender, long capacity, long remaining) throws IOException;

    /**
     * blockReport()告诉NameNode关于所有本地存储的块。
     * NameNode返回一个已经过时且应该删除的块数组。这个函数的作用是上传“所有”本地存储的块。
     * 它在启动时调用，之后很少调用。
     * @param sender
     * @param blocks
     * @return
     * @throws IOException
     */
    Block[] blockReport(String sender, Block[] blocks) throws IOException;

    /**
     * blockReceived()允许DataNode将最近接收的块数据告诉NameNode。
     * 例如，每当客户机代码在这里写入一个新块，或者另一个DataNode将一个块复制到这个DataNode时，它将调用blockReceived()。
     * @param sender
     * @param blocks
     * @throws IOException
     */
    void blockReceived(String sender, Block[] blocks) throws IOException;

    /**
     * errorReport()告诉NameNode发生了错误。
     * 用于调试。
     * @param sender
     * @param msg
     * @throws IOException
     */
    void errorReport(String sender, String msg) throws IOException;

    /**
     * The DataNode periodically calls getBlockwork().  It includes a
     * small amount of status information, but mainly gives the NameNode
     * a chance to return a "BlockCommand" object.  A BlockCommand tells
     * the DataNode to invalidate local block(s), or to copy them to other
     * DataNodes, etc.
     */
    /**
     * DataNode定期调用getBlockwork()。
     * 它包含少量的状态信息，但主要给了NameNode一个返回“BlockCommand”对象的机会。
     * 块命令告诉DataNode使本地块失效，或者将它们复制到其他DataNode，等等。
     * @param sender
     * @param xmitsInProgress
     * @return
     * @throws IOException
     */
    BlockCommand getBlockwork(String sender, int xmitsInProgress) throws IOException;
}
