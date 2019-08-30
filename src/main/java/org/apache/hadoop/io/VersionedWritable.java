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

import java.io.DataOutput;
import java.io.DataInput;
import java.io.IOException;

/**
 * 用于提供版本检查的可写程序的基类。<br/>
 * 这在类可能进化时很有用，因此实例由<br/>
 * 类的旧版本仍然可以由新版本处理。<br/>
 * 要处理这种情况，{@link #readFields(DataInput)}<br/>
 * 实现应该捕获{@link VersionMismatchException}。
 * @author 章云
 * @date 2019/8/30 9:48
 */
public abstract class VersionedWritable implements Writable {

    /**
     * 返回当前实现的版本号。
     * @return
     */
    public abstract byte getVersion();

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeByte(getVersion());
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        byte version = in.readByte();
        if (version != getVersion()) {
            throw new VersionMismatchException(getVersion(), version);
        }
    }


}
