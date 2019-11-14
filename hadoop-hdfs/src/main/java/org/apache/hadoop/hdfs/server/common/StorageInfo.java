/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hdfs.server.common;


/**
 * 存储文件信息
 * @author 章云
 * @date 2019/6/19 21:43
 */
public class StorageInfo {
    /**
     * 存储文件版本
     */
    public int layoutVersion;
    /**
     * namespaceID
     */
    public int namespaceID;
    /**
     * 存储文件创建的时间戳
     */
    public long cTime;

    public StorageInfo() {
        this(0, 0, 0L);
    }

    public StorageInfo(int layoutV, int nsID, long cT) {
        layoutVersion = layoutV;
        namespaceID = nsID;
        cTime = cT;
    }

    /**
     * 克隆
     * @param from
     */
    public StorageInfo(StorageInfo from) {
        setStorageInfo(from);
    }

    public int getLayoutVersion() {
        return layoutVersion;
    }

    public int getNamespaceID() {
        return namespaceID;
    }

    public long getCTime() {
        return cTime;
    }

    public void setStorageInfo(StorageInfo from) {
        layoutVersion = from.layoutVersion;
        namespaceID = from.namespaceID;
        cTime = from.cTime;
    }
}