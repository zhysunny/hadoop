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

package org.apache.hadoop.io;

import java.io.IOException;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.DataInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ByteArrayInputStream;
import java.util.zip.Deflater;
import java.util.zip.DeflaterOutputStream;
import java.util.zip.InflaterInputStream;

/**
 * 一个用于可写文件的基类，可写文件本身在字段访问时被压缩和惰性膨胀。<br/>
 * 这对于在map或reduce操作期间字段未被更改的大型对象非常有用:<br/>
 * 将字段数据压缩后，可以更快地将实例从一个文件复制到另一个文件。
 * @author 章云
 * @date 2019/8/30 9:30
 */
public abstract class CompressedWritable implements Writable {
    /**
     * 如果非空，则压缩此实例的字段数据。
     */
    private byte[] compressed;

    public CompressedWritable() {
    }

    @Override
    public final void readFields(DataInput in) throws IOException {
        compressed = new byte[in.readInt()];
        in.readFully(compressed, 0, compressed.length);
    }

    /**
     * 必须由访问字段的所有方法调用，以确保数据已解压缩。
     */
    protected void ensureInflated() {
        if (compressed != null) {
            try {
                ByteArrayInputStream deflated = new ByteArrayInputStream(compressed);
                DataInput inflater = new DataInputStream(new InflaterInputStream(deflated));
                readFieldsCompressed(inflater);
                compressed = null;
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }

    /**
     * 子类实现了这一点，而不是{@link #readFields(DataInput)}。
     * @param in
     * @throws IOException
     */
    protected abstract void readFieldsCompressed(DataInput in) throws IOException;

    @Override
    public final void write(DataOutput out) throws IOException {
        if (compressed == null) {
            ByteArrayOutputStream deflated = new ByteArrayOutputStream();
            Deflater deflater = new Deflater(Deflater.BEST_SPEED);
            DataOutputStream dout = new DataOutputStream(new DeflaterOutputStream(deflated, deflater));
            writeCompressed(dout);
            dout.close();
            compressed = deflated.toByteArray();
        }
        out.writeInt(compressed.length);
        out.write(compressed);
    }

    /**
     * 子类实现了这一点，而不是{@link #write(DataOutput)}。
     * @param out
     * @throws IOException
     */
    protected abstract void writeCompressed(DataOutput out) throws IOException;

}
