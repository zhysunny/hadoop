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

import java.io.*;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileUtil;

/**
 * FSDirectory存储文件系统目录状态。<br/>
 * 它处理向磁盘写入/加载值，并记录更改。<br/>
 * 它保持文件名->块集映射始终是当前的，并记录到磁盘。
 * @author 章云
 * @date 2019/8/10 11:03
 */
public class FSDirectory implements FSConstants {
    static String FS_IMAGE = "fsimage";
    static String NEW_FS_IMAGE = "fsimage.new";
    static String OLD_FS_IMAGE = "fsimage.old";

    private static final byte OP_ADD = 0;
    private static final byte OP_RENAME = 1;
    private static final byte OP_DELETE = 2;
    private static final byte OP_MKDIR = 3;

    /******************************************************
     * 我们在内存中保存文件/块层次结构的表示。组合模式
     ******************************************************/
    public class INode {
        public String name;
        public INode parent;
        /**
         * key值存储的是文件名
         */
        public TreeMap<String, INode> children = new TreeMap<String, INode>();
        public Block[] blocks;

        INode(String name, INode parent, Block[] blocks) {
            this.name = name;
            this.parent = parent;
            this.blocks = blocks;
        }

        /**
         * 检查它是否是一个目录
         * @return
         */
        synchronized public boolean isDir() {
            return (blocks == null);
        }

        /**
         * 这是外部接口
         */
        INode getNode(String target) {
            if (!target.startsWith("/") || target.length() == 0) {
                return null;
            } else if (parent == null && "/".equals(target)) {
                return this;
            } else {
                Vector<String> components = new Vector<String>();
                int start = 0;
                int slashid = 0;
                while (start < target.length() && (slashid = target.indexOf('/', start)) >= 0) {
                    components.add(target.substring(start, slashid));
                    start = slashid + 1;
                }
                if (start < target.length()) {
                    components.add(target.substring(start));
                }
                return getNode(components, 0);
            }
        }

        INode getNode(Vector<String> components, int index) {
            if (!name.equals(components.elementAt(index))) {
                return null;
            }
            if (index == components.size() - 1) {
                return this;
            }
            // Check with children
            INode child = children.get(components.elementAt(index + 1));
            if (child == null) {
                return null;
            } else {
                return child.getNode(components, index + 1);
            }
        }

        INode addNode(String target, Block[] blks) {
            if (getNode(target) != null) {
                // 文件或目录已存在
                return null;
            } else {
                String parentName = DFSFile.getDFSParent(target);
                if (parentName == null) {
                    return null;
                }
                INode parentNode = getNode(parentName);
                if (parentNode == null) {
                    // 无论文件还是目录，智能一层一层的添加
                    return null;
                } else {
                    String targetName = new File(target).getName();
                    INode newItem = new INode(targetName, parentNode, blks);
                    parentNode.children.put(targetName, newItem);
                    return newItem;
                }
            }
        }

        boolean removeNode() {
            if (parent == null) {
                return false;
            } else {
                parent.children.remove(name);
                return true;
            }
        }

        /**
         * 在这个INode中收集所有块及其所有子节点。<br/>
         * 此操作是在从树中删除节点之后执行的，我们希望GC此节点及其以下的所有块。
         */
        void collectSubtreeBlocks(Vector<Block> v) {
            if (blocks != null) {
                for (int i = 0; i < blocks.length; i++) {
                    v.add(blocks[i]);
                }
            }
            for (Iterator<INode> it = children.values().iterator(); it.hasNext(); ) {
                INode child = it.next();
                child.collectSubtreeBlocks(v);
            }
        }

        /**
         * 所有子节点的个数，自身算一个
         * @return
         */
        int numItemsInTree() {
            int total = 0;
            for (Iterator<INode> it = children.values().iterator(); it.hasNext(); ) {
                INode child = it.next();
                total += child.numItemsInTree();
            }
            return total + 1;
        }

        /**
         * 获取节点的全路径
         * @return
         */
        String computeName() {
            if (parent != null) {
                return parent.computeName() + "/" + name;
            } else {
                return name;
            }
        }

        /**
         * 当前节点对应block的大小
         * @return
         */
        long computeFileLength() {
            long total = 0;
            if (blocks != null) {
                // 目录长度默认为0，这里计算每个block的大小
                for (int i = 0; i < blocks.length; i++) {
                    total += blocks[i].getNumBytes();
                }
            }
            return total;
        }

        /**
         * 当前节点和所有子节点的block大小和
         * @return
         */
        long computeContentsLength() {
            long total = computeFileLength();
            for (Iterator<INode> it = children.values().iterator(); it.hasNext(); ) {
                INode child = it.next();
                total += child.computeContentsLength();
            }
            return total;
        }

        /**
         * 所有子节点
         * @param v
         */
        void listContents(Vector<INode> v) {
            if (parent != null && blocks != null) {
                v.add(this);
            }
            for (Iterator<INode> it = children.values().iterator(); it.hasNext(); ) {
                INode child = it.next();
                v.add(child);
            }
        }

        /**
         * 进程启动的时候将INode节点保存为fsimage.new文件中
         * @param parentPrefix
         * @param out
         * @throws IOException
         */
        void saveImage(String parentPrefix, DataOutputStream out) throws IOException {
            String fullName = "";
            if (parent != null) {
                fullName = parentPrefix + "/" + name;
                new UTF8(fullName).write(out);
                if (blocks == null) {
                    out.writeInt(0);
                } else {
                    out.writeInt(blocks.length);
                    for (int i = 0; i < blocks.length; i++) {
                        blocks[i].write(out);
                    }
                }
            }
            for (Iterator<INode> it = children.values().iterator(); it.hasNext(); ) {
                INode child = it.next();
                child.saveImage(fullName, out);
            }
        }

        @Override
        public String toString() {
            StringBuffer sb = new StringBuffer(256);
            toString(this, sb);
            return sb.toString();
        }

        private void toString(INode iNode, StringBuffer sb) {
            sb.append("-----------------------------------\n");
            sb.append("Name: ").append(iNode.name).append('\n');
            if (iNode.parent != null) {
                sb.append("Parent: ").append(iNode.parent.name).append('\n');
            } else {
                sb.append("Parent: null").append('\n');
            }
            if (iNode.blocks != null) {
                sb.append("Blocks: ").append(iNode.blocks.length).append('\n');
            } else {
                sb.append("Blocks: null").append('\n');
            }
            for (Map.Entry<String, INode> entry : iNode.children.entrySet()) {
                sb.append(entry.getKey()).append(':').append('\n');
                toString(entry.getValue(), sb);
            }
        }
    }

    INode rootDir = new INode("", null, null);
    TreeSet<Block> activeBlocks = new TreeSet<Block>();
    TreeMap<UTF8, TreeSet<UTF8>> activeLocks = new TreeMap<UTF8, TreeSet<UTF8>>();
    DataOutputStream editlog = null;
    boolean ready = false;

    /**
     * 进程启动是初始化FSDirectory实例
     * @param dir namenode存储路径
     * @throws IOException
     */
    public FSDirectory(File dir) throws IOException {
        File fullimage = new File(dir, "image");
        if (!fullimage.exists()) {
            // 没有-format
            throw new IOException("NameNode not formatted: " + dir);
        }
        File edits = new File(dir, "edits");
        // edits信息更新到fsimage只有进程启动时执行，当edits文件过大导致进程启动很慢，这就是后面出现secondarynamenode的原因
        if (loadFSImage(fullimage, edits)) {
            // 当edits中有新的更新信息，将INode数据重新保存到fsimage中
            saveFSImage(fullimage, edits);
        }
        // 初始化完成，namenode启动成功，开启editlog输出流
        synchronized (this) {
            this.ready = true;
            this.notifyAll();
            this.editlog = new DataOutputStream(new FileOutputStream(edits));
        }
    }

    /**
     * 格式化一个新的文件系统。销毁可能已经存在于此位置的任何文件系统。
     * @param dir  namenode存储目录
     * @param conf 配置类
     * @throws IOException
     */
    public static void format(File dir, Configuration conf) throws IOException {
        File image = new File(dir, "image");
        File edits = new File(dir, "edits");
        if (!((!image.exists() || FileUtil.fullyDelete(image, conf)) &&
                (!edits.exists() || edits.delete()) &&
                image.mkdirs())) {
            // !((!image.exists() || FileUtil.fullyDelete(image, conf)) && (!edits.exists() || edits.delete()) && image.mkdirs())
            // !(!image.exists() || FileUtil.fullyDelete(image, conf)) || !(!edits.exists() || edits.delete()) || !image.mkdirs()
            // (image.exists() && !FileUtil.fullyDelete(image, conf)) || (edits.exists() && !edits.delete()) || !image.mkdirs()
            // image目录存在且不能删除  或者  edits存在且不能删除  或者  image目录不能创建
            // 删除文件失败，抛出异常
            throw new IOException("Unable to format: " + dir);
        }
    }

    /**
     * 关闭filestore
     */
    public void close() throws IOException {
        editlog.close();
    }

    /**
     * 当进程启动还在加载文件信息时，有新的文件更新操作，会暂时等待，直到namenode加载完数据到INode
     */
    void waitForReady() {
        if (!ready) {
            synchronized (this) {
                while (!ready) {
                    try {
                        this.wait(5000);
                    } catch (InterruptedException ie) {
                    }
                }
            }
        }
    }

    /**
     * 加载文件系统映像。它是一个由文件名和块组成的大列表。<br/>
     * 返回是否应该“重新保存”并合并编辑日志
     * @param fsdir image目录
     * @param edits edits文件
     * @return
     * @throws IOException
     */
    boolean loadFSImage(File fsdir, File edits) throws IOException {
        // 原子移动顺序，从中断中恢复保存
        File curFile = new File(fsdir, FS_IMAGE);
        File newFile = new File(fsdir, NEW_FS_IMAGE);
        File oldFile = new File(fsdir, OLD_FS_IMAGE);
        // 中断恢复过程
        if (oldFile.exists() && curFile.exists()) {
            // 也许我们在2到4之间被打断了
            oldFile.delete();
            if (edits.exists()) {
                edits.delete();
            }
        } else if (oldFile.exists() && newFile.exists()) {
            // 或者在1和2之间
            newFile.renameTo(curFile);
            oldFile.delete();
        } else if (curFile.exists() && newFile.exists()) {
            // 或者在阶段1之前，在这种情况下，我们将丢失编辑
            newFile.delete();
        }
        if (curFile.exists()) {
            // 将当前的fsimage文件加载到INode中
            DataInputStream in = new DataInputStream(new BufferedInputStream(new FileInputStream(curFile)));
            try {
                int numFiles = in.readInt();
                for (int i = 0; i < numFiles; i++) {
                    UTF8 name = new UTF8();
                    name.readFields(in);
                    int numBlocks = in.readInt();
                    if (numBlocks == 0) {
                        unprotectedAddFile(name, null);
                    } else {
                        Block[] blocks = new Block[numBlocks];
                        for (int j = 0; j < numBlocks; j++) {
                            blocks[j] = new Block();
                            blocks[j].readFields(in);
                        }
                        unprotectedAddFile(name, blocks);
                    }
                }
            } finally {
                in.close();
            }
        }
        if (edits.exists() && loadFSEdits(edits) > 0) {
            // 如果edits有文件变换信息，返回true，loadFSEdits会将edits文件更新到INode中
            return true;
        } else {
            return false;
        }
    }

    /**
     * 加载编辑日志，并将更改应用于内存结构<br/>
     * 这是我们应用编辑的地方，我们一直写磁盘。
     */
    int loadFSEdits(File edits) throws IOException {
        int numEdits = 0;
        if (edits.exists()) {
            DataInputStream in = new DataInputStream(new BufferedInputStream(new FileInputStream(edits)));
            try {
                while (in.available() > 0) {
                    byte opcode = in.readByte();
                    numEdits++;
                    switch (opcode) {
                        case OP_ADD: {
                            // edits文件中的新增文件信息
                            UTF8 name = new UTF8();
                            name.readFields(in);
                            ArrayWritable aw = new ArrayWritable(Block.class);
                            aw.readFields(in);
                            Writable[] writables = aw.get();
                            Block[] blocks = new Block[writables.length];
                            System.arraycopy(writables, 0, blocks, 0, blocks.length);
                            unprotectedAddFile(name, blocks);
                            break;
                        }
                        case OP_RENAME: {
                            // edits文件中的重命名文件信息
                            UTF8 src = new UTF8();
                            UTF8 dst = new UTF8();
                            src.readFields(in);
                            dst.readFields(in);
                            unprotectedRenameTo(src, dst);
                            break;
                        }
                        case OP_DELETE: {
                            // edits文件中的删除文件信息
                            UTF8 src = new UTF8();
                            src.readFields(in);
                            unprotectedDelete(src);
                            break;
                        }
                        case OP_MKDIR: {
                            // edits文件中的目录创建信息
                            UTF8 src = new UTF8();
                            src.readFields(in);
                            unprotectedMkdir(src.toString());
                            break;
                        }
                        default: {
                            throw new IOException("Never seen opcode " + opcode);
                        }
                    }
                }
            } finally {
                in.close();
            }
        }
        return numEdits;
    }

    /**
     * 保存FS映像的内容
     */
    void saveFSImage(File fullimage, File edits) throws IOException {
        File curFile = new File(fullimage, FS_IMAGE);
        File newFile = new File(fullimage, NEW_FS_IMAGE);
        File oldFile = new File(fullimage, OLD_FS_IMAGE);
        // 将INode内存信息保存到fsimage.new文件中
        DataOutputStream out = new DataOutputStream(new BufferedOutputStream(new FileOutputStream(newFile)));
        try {
            out.writeInt(rootDir.numItemsInTree() - 1);
            rootDir.saveImage("", out);
        } finally {
            out.close();
        }
        // 原子移动序列
        // 1.  Move cur to old
        curFile.renameTo(oldFile);
        // 2.  Move new to cur
        newFile.renameTo(curFile);
        // 3.  Remove pending-edits file (it's been integrated with newFile)
        edits.delete();
        // 4.  Delete old
        oldFile.delete();
    }

    /**
     * 向editlog写入操作
     */
    private void logEdit(byte op, Writable w1, Writable w2) {
        synchronized (editlog) {
            try {
                editlog.write(op);
                if (w1 != null) {
                    w1.write(editlog);
                }
                if (w2 != null) {
                    w2.write(editlog);
                }
            } catch (IOException ie) {
            }
        }
    }

    /**
     * 将给定的文件名添加到fs。
     */
    public boolean addFile(UTF8 src, Block[] blocks) {
        waitForReady();
        // 始终为父目录树执行隐式mkdirs
        mkdirs(DFSFile.getDFSParent(src.toString()));
        if (unprotectedAddFile(src, blocks)) {
            logEdit(OP_ADD, src, new ArrayWritable(Block.class, blocks));
            return true;
        } else {
            return false;
        }
    }

    private boolean unprotectedAddFile(UTF8 name, Block[] blocks) {
        synchronized (rootDir) {
            if (blocks != null) {
                // 添加文件- >块映射
                for (int i = 0; i < blocks.length; i++) {
                    activeBlocks.add(blocks[i]);
                }
            }
            return rootDir.addNode(name.toString(), blocks) != null;
        }
    }

    /**
     * 更改文件名
     */
    public boolean renameTo(UTF8 src, UTF8 dst) {
        waitForReady();
        if (unprotectedRenameTo(src, dst)) {
            logEdit(OP_RENAME, src, dst);
            return true;
        } else {
            return false;
        }
    }

    private boolean unprotectedRenameTo(UTF8 src, UTF8 dst) {
        synchronized (rootDir) {
            INode removedNode = rootDir.getNode(src.toString());
            if (removedNode == null) {
                return false;
            }
            removedNode.removeNode();
            if (isDir(dst)) {
                dst = new UTF8(dst.toString() + "/" + new File(src.toString()).getName());
            }
            INode newNode = rootDir.addNode(dst.toString(), removedNode.blocks);
            if (newNode != null) {
                newNode.children = removedNode.children;
                for (Iterator<INode> it = newNode.children.values().iterator(); it.hasNext(); ) {
                    INode child = it.next();
                    child.parent = newNode;
                }
                return true;
            } else {
                rootDir.addNode(src.toString(), removedNode.blocks);
                return false;
            }
        }
    }

    /**
     * 从管理中删除文件，返回块
     */
    public Block[] delete(UTF8 src) {
        waitForReady();
        logEdit(OP_DELETE, src, null);
        return unprotectedDelete(src);
    }

    private Block[] unprotectedDelete(UTF8 src) {
        synchronized (rootDir) {
            INode targetNode = rootDir.getNode(src.toString());
            if (targetNode == null) {
                return null;
            } else {
                // 将节点从名称空间中移除，并对节点下面的所有块进行GC。
                if (!targetNode.removeNode()) {
                    return null;
                } else {
                    Vector<Block> v = new Vector<Block>();
                    targetNode.collectSubtreeBlocks(v);
                    for (Iterator<Block> it = v.iterator(); it.hasNext(); ) {
                        Block b = it.next();
                        activeBlocks.remove(b);
                    }
                    return v.toArray(new Block[v.size()]);
                }
            }
        }
    }

    public int obtainLock(UTF8 src, UTF8 holder, boolean exclusive) {
        TreeSet<UTF8> holders = activeLocks.get(src);
        if (holders == null) {
            holders = new TreeSet<UTF8>();
            activeLocks.put(src, holders);
        }
        if (exclusive && holders.size() > 0) {
            return STILL_WAITING;
        } else {
            holders.add(holder);
            return COMPLETE_SUCCESS;
        }
    }

    public int releaseLock(UTF8 src, UTF8 holder) {
        TreeSet<UTF8> holders = activeLocks.get(src);
        if (holders != null && holders.contains(holder)) {
            holders.remove(holder);
            if (holders.size() == 0) {
                activeLocks.remove(src);
            }
            return COMPLETE_SUCCESS;
        } else {
            return OPERATION_FAILED;
        }
    }

    /**
     * 获取给定路径“src”的文件列表<br/>
     * 这个函数现在确实效率很低。<br/>
     * 我们以后会做得更好。
     */
    public DFSFileInfo[] getListing(UTF8 src) {
        String srcs = normalizePath(src);
        synchronized (rootDir) {
            INode targetNode = rootDir.getNode(srcs);
            if (targetNode == null) {
                return null;
            } else {
                Vector<INode> contents = new Vector<INode>();
                targetNode.listContents(contents);
                DFSFileInfo[] listing = new DFSFileInfo[contents.size()];
                int i = 0;
                for (Iterator<INode> it = contents.iterator(); it.hasNext(); i++) {
                    listing[i] = new DFSFileInfo(it.next());
                }
                return listing;
            }
        }
    }

    /**
     * 获取与文件关联的块
     */
    public Block[] getFile(UTF8 src) {
        waitForReady();
        synchronized (rootDir) {
            INode targetNode = rootDir.getNode(src.toString());
            if (targetNode == null) {
                return null;
            } else {
                return targetNode.blocks;
            }
        }
    }

    /**
     * 检查是否可以创建文件路径
     */
    public boolean isValidToCreate(UTF8 src) {
        String srcs = normalizePath(src);
        synchronized (rootDir) {
            if (srcs.startsWith("/") &&
                    !srcs.endsWith("/") &&
                    rootDir.getNode(srcs) == null) {
                return true;
            } else {
                return false;
            }
        }
    }

    /**
     * 检查路径是否指定了目录
     */
    public boolean isDir(UTF8 src) {
        synchronized (rootDir) {
            INode node = rootDir.getNode(normalizePath(src));
            return node != null && node.isDir();
        }
    }

    /**
     * 创建给定目录及其所有父目录。
     */
    public boolean mkdirs(UTF8 src) {
        return mkdirs(src.toString());
    }

    /**
     * 创建多级目录时一层一层的创建目录
     */
    boolean mkdirs(String src) {
        src = normalizePath(new UTF8(src));
        // 使用它来收集我们需要构造的所有dirs
        Vector<String> v = new Vector<String>();
        // dir本身
        v.add(src);
        // All its parents
        String parent = DFSFile.getDFSParent(src);
        while (parent != null) {
            v.add(parent);
            parent = DFSFile.getDFSParent(parent);
        }
        // 现在回过头来看看dirs列表，一路创建
        boolean lastSuccess = false;
        int numElts = v.size();
        for (int i = numElts - 1; i >= 0; i--) {
            String cur = v.elementAt(i);
            INode inserted = unprotectedMkdir(cur);
            if (inserted != null) {
                logEdit(OP_MKDIR, new UTF8(inserted.computeName()), null);
                lastSuccess = true;
            } else {
                lastSuccess = false;
            }
        }
        return lastSuccess;
    }

    /**
     * 创建目录节点
     * @param src
     * @return
     */
    INode unprotectedMkdir(String src) {
        synchronized (rootDir) {
            return rootDir.addNode(src, null);
        }
    }

    /**
     * 去掉最后一个/
     * @param src
     * @return
     */
    String normalizePath(UTF8 src) {
        String srcs = src.toString();
        if (srcs.length() > 1 && srcs.endsWith("/")) {
            srcs = srcs.substring(0, srcs.length() - 1);
        }
        return srcs;
    }

    /**
     * 返回给定块是否为文件指向的块。
     */
    public boolean isValidBlock(Block b) {
        synchronized (rootDir) {
            if (activeBlocks.contains(b)) {
                return true;
            } else {
                return false;
            }
        }
    }
}
