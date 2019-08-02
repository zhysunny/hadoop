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
 * 基于{@link DataInput}和{@link DataOutput}的简单、高效的序列化协议。<br/>
 * 实现通常实现一个静态的read(DataInput)方法，该方法构造一个新实例，调用{@link #readFields(DataInput)}并返回实例。
 * @author 章云
 * @date 2019/8/1 8:58
 */
public interface Writable {
    /**
     * 将此对象的字段写到out。
     * @param out
     * @throws IOException
     */
    void write(DataOutput out) throws IOException;

    /**
     * 从中的中读取此对象的字段。<br/>
     * 为了提高效率，实现应该尽可能重用现有对象中的存储。
     * @param in
     * @throws IOException
     */
    void readFields(DataInput in) throws IOException;
}
