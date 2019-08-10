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

/**
 * DFSFileInfo跟踪关于远程文件的信息，包括名称、大小等。
 * @author 章云
 * @date 2019/8/9 14:05
 */
class DFSFileInfo implements Writable {
    static {
        WritableFactories.setFactory(DFSFileInfo.class, new WritableFactory() {
            @Override
            public Writable newInstance() {
                return new DFSFileInfo();
            }
        });
    }

    UTF8 path;
    long len;
    long contentsLen;
    boolean isDir;

    public DFSFileInfo() {
    }

    /**
     * 由档案INode创建DFSFileInfo
     */
    public DFSFileInfo(FSDirectory.INode node) {
        this.path = new UTF8(node.computeName());
        this.isDir = node.isDir();
        if (isDir) {
            this.len = 0;
            this.contentsLen = node.computeContentsLength();
        } else {
            this.len = this.contentsLen = node.computeFileLength();
        }
    }

    public String getPath() {
        return path.toString();
    }

    public String getName() {
        return new File(path.toString()).getName();
    }

    public String getParent() {
        return DFSFile.getDFSParent(path.toString());
    }

    public long getLen() {
        return len;
    }

    public long getContentsLen() {
        return contentsLen;
    }

    public boolean isDir() {
        return isDir;
    }

    //////////////////////////////////////////////////
    // Writable
    //////////////////////////////////////////////////
    @Override
    public void write(DataOutput out) throws IOException {
        path.write(out);
        out.writeLong(len);
        out.writeLong(contentsLen);
        out.writeBoolean(isDir);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.path = new UTF8();
        this.path.readFields(in);
        this.len = in.readLong();
        this.contentsLen = in.readLong();
        this.isDir = in.readBoolean();
    }
}

