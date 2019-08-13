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
 * LocatedBlock是一对Block DatanodeInfo[]对象。<br/>
 * 它告诉我们在哪里可以找到一个Block。
 * @author 章云
 * @date 2019/8/9 8:45
 */
public class LocatedBlock implements Writable {

    static {
        WritableFactories.setFactory(LocatedBlock.class, new WritableFactory() {
            @Override
            public Writable newInstance() {
                return new LocatedBlock();
            }
        });
    }

    private Block b;
    private DatanodeInfo[] locs;

    public LocatedBlock() {
        this.b = new Block();
        this.locs = new DatanodeInfo[0];
    }

    public LocatedBlock(Block b, DatanodeInfo[] locs) {
        this.b = b;
        this.locs = locs;
    }

    public Block getBlock() {
        return b;
    }

    DatanodeInfo[] getLocations() {
        return locs;
    }

    ///////////////////////////////////////////
    // Writable
    ///////////////////////////////////////////
    @Override
    public void write(DataOutput out) throws IOException {
        b.write(out);
        out.writeInt(locs.length);
        for (int i = 0; i < locs.length; i++) {
            locs[i].write(out);
        }
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.b = new Block();
        b.readFields(in);
        int count = in.readInt();
        this.locs = new DatanodeInfo[count];
        for (int i = 0; i < locs.length; i++) {
            locs[i] = new DatanodeInfo();
            locs[i].readFields(in);
        }
    }
}
