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
package org.apache.hadoop.fs;

import java.io.*;

/**
 * FSInputStream是一个通用的旧InputStream，具有一点RAF-style的搜索功能。
 * @author 章云
 * @date 2019/8/2 13:24
 */
public abstract class FSInputStream extends InputStream implements Seekable {

    /**
     * 从文件开始查找给定的偏移量。下一个read()将来自该位置。无法查找文件末尾之后的内容。
     * @param pos
     * @throws IOException
     */
    @Override
    public abstract void seek(long pos) throws IOException;

    /**
     * 返回从文件开始的当前偏移量
     * @return
     * @throws IOException
     */
    public abstract long getPos() throws IOException;
}
