/**
 * Copyright 2006 The Apache Software Foundation
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

/**
 * 可以关闭的<br/>
 * 升级到Java 1.5时替换为java.io.Closeable。
 * @author 章云
 * @date 2019/8/1 16:44
 */
public interface Closeable {
    /**
     * 在最后一次调用此对象上的任何其他方法后调用，以释放和/或刷新资源。典型的实现什么也不做。
     * @throws IOException
     */
    void close() throws IOException;
}
