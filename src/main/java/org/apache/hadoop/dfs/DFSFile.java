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

import java.io.*;

/**
 * DFSFile是一个传统的java文件，它已经用一些额外的信息进行了注释。<br/>
 * 对 {@link DFSFileInfo} 的封装
 * @author 章云
 * @date 2019/8/9 14:01
 */
public class DFSFile extends File {
    DFSFileInfo info;

    /**
     * DFS文件名中使用的分隔符。
     */
    public static final String DFS_FILE_SEPARATOR = "/";

    public DFSFile(DFSFileInfo info) {
        super(info.getPath());
        this.info = info;
    }

    /**
     * 这个子类中不支持许多文件方法
     */
    @Override
    public boolean canRead() {
        return false;
    }

    @Override
    public boolean canWrite() {
        return false;
    }

    @Override
    public boolean createNewFile() {
        return false;
    }

    @Override
    public boolean delete() {
        return false;
    }

    @Override
    public void deleteOnExit() {
    }

    @Override
    public boolean isHidden() {
        return false;
    }

    @Override
    public boolean isAbsolute() {
        return true;
    }

    /**
     * 我们需要重新实施其中的一些
     */
    @Override
    public boolean isDirectory() {
        return info.isDir();
    }

    @Override
    public boolean isFile() {
        return !isDirectory();
    }

    @Override
    public long length() {
        return info.getLen();
    }

    /**
     * 并添加一些额外的功能
     */
    public long getContentsLength() {
        return info.getContentsLen();
    }

    /**
     * 从DFS路径字符串检索父路径
     * @param path DFS路径
     * @return DFS路径的父路径，如果不存在父路径，则为null。
     */
    public static String getDFSParent(String path) {
        if (path == null) {
            return null;
        }
        if (DFS_FILE_SEPARATOR.equals(path)) {
            return null;
        }
        int index = path.lastIndexOf(DFS_FILE_SEPARATOR);
        if (index == -1) {
            return null;
        }
        if (index == 0) {
            return DFS_FILE_SEPARATOR;
        }
        return path.substring(0, index);
    }
}
