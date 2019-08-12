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

/**
 * 块是Hadoop FS原语，由long标识。
 * @author 章云
 * @date 2019/8/8 20:29
 */
public class Block implements Writable, Comparable {

    static {
        WritableFactories.setFactory(Block.class, new WritableFactory() {
            @Override
            public Writable newInstance() {
                return new Block();
            }
        });
    }

    static Random r = new Random();

    /**
     * 是否是block文件
     * @param f
     * @return
     */
    public static boolean isBlockFilename(File f) {
        if (f.getName().startsWith("blk_")) {
            return true;
        } else {
            return false;
        }
    }

    long blkid;
    long len;

    public Block() {
        this.blkid = r.nextLong();
        this.len = 0;
    }

    public Block(long blkid, long len) {
        this.blkid = blkid;
        this.len = len;
    }

    /**
     * 从给定的文件名中找到blockid
     * @param f
     * @param len f.length()
     */
    public Block(File f, long len) {
        String name = f.getName();
        name = name.substring("blk_".length());
        this.blkid = Long.parseLong(name);
        this.len = len;
    }

    public long getBlockId() {
        return blkid;
    }

    public String getBlockName() {
        return "blk_" + String.valueOf(blkid);
    }

    public long getNumBytes() {
        return len;
    }

    public void setNumBytes(long len) {
        this.len = len;
    }

    @Override
    public String toString() {
        return getBlockName();
    }

    /////////////////////////////////////
    // Writable
    /////////////////////////////////////
    @Override
    public void write(DataOutput out) throws IOException {
        out.writeLong(blkid);
        out.writeLong(len);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.blkid = in.readLong();
        this.len = in.readLong();
    }

    /////////////////////////////////////
    // Comparable
    /////////////////////////////////////
    @Override
    public int compareTo(Object o) {
        // blkid升序排序
        Block b = (Block) o;
        if (getBlockId() < b.getBlockId()) {
            return -1;
        } else if (getBlockId() == b.getBlockId()) {
            return 0;
        } else {
            return 1;
        }
    }

    @Override
    public boolean equals(Object o) {
        Block b = (Block) o;
        // blkid相等，两个对象就相等
        return (this.compareTo(b) == 0);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(this.blkid);
    }
}
