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

import java.io.DataInput;
import java.io.IOException;

/**
 * 当被读取对象的版本与{@link VersionedWritable#getVersion()}返回的当前实现版本不匹配时，由{@link VersionedWritable#readFields(DataInput)}抛出。
 * @author 章云
 * @date 2019/8/30 9:50
 */
public class VersionMismatchException extends IOException {

    private byte expectedVersion;
    private byte foundVersion;

    public VersionMismatchException(byte expectedVersionIn, byte foundVersionIn) {
        expectedVersion = expectedVersionIn;
        foundVersion = foundVersionIn;
    }

    @Override
    public String toString() {
        return "A record version mismatch occured. Expecting v" + expectedVersion + ", found v" + foundVersion;
    }
}
