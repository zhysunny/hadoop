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
 * 块命令是针对datanode所控制的某些块的一条指令。<br/>
 * 它告诉DataNode使一组指示的块无效，或者将一组指示的块复制到另一个DataNode。
 * @author 章云
 * @date 2019/8/8 20:49
 */
public class BlockCommand implements Writable {

    static {
        WritableFactories.setFactory(BlockCommand.class, new WritableFactory() {
            @Override
            public Writable newInstance() {
                return new BlockCommand();
            }
        });
    }

    boolean transferBlocks;
    boolean invalidateBlocks;
    Block[] blocks;
    DatanodeInfo[][] targets;

    public BlockCommand() {
        this.transferBlocks = false;
        this.invalidateBlocks = false;
        this.blocks = new Block[0];
        this.targets = new DatanodeInfo[0][];
    }

    public BlockCommand(Block[] blocks, DatanodeInfo[][] targets) {
        this.transferBlocks = true;
        this.invalidateBlocks = false;
        this.blocks = blocks;
        this.targets = targets;
    }

    public BlockCommand(Block[] blocks) {
        this.transferBlocks = false;
        this.invalidateBlocks = true;
        this.blocks = blocks;
        this.targets = new DatanodeInfo[0][];
    }

    public boolean transferBlocks() {
        return transferBlocks;
    }

    public boolean invalidateBlocks() {
        return invalidateBlocks;
    }

    public Block[] getBlocks() {
        return blocks;
    }

    public DatanodeInfo[][] getTargets() {
        return targets;
    }

    ///////////////////////////////////////////
    // Writable
    ///////////////////////////////////////////
    @Override
    public void write(DataOutput out) throws IOException {
        out.writeBoolean(transferBlocks);
        out.writeBoolean(invalidateBlocks);
        out.writeInt(blocks.length);
        for (int i = 0; i < blocks.length; i++) {
            blocks[i].write(out);
        }
        out.writeInt(targets.length);
        for (int i = 0; i < targets.length; i++) {
            out.writeInt(targets[i].length);
            for (int j = 0; j < targets[i].length; j++) {
                targets[i][j].write(out);
            }
        }
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.transferBlocks = in.readBoolean();
        this.invalidateBlocks = in.readBoolean();
        this.blocks = new Block[in.readInt()];
        for (int i = 0; i < blocks.length; i++) {
            blocks[i] = new Block();
            blocks[i].readFields(in);
        }
        this.targets = new DatanodeInfo[in.readInt()][];
        for (int i = 0; i < targets.length; i++) {
            this.targets[i] = new DatanodeInfo[in.readInt()];
            for (int j = 0; j < targets[i].length; j++) {
                targets[i][j] = new DatanodeInfo();
                targets[i][j].readFields(in);
            }
        }
    }
}
