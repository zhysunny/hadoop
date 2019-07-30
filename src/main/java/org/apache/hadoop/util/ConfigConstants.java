package org.apache.hadoop.util;

/**
 * 配置常量类
 * @author 章云
 * @date 2019/7/30 15:30
 */
public class ConfigConstants {

    public static final String DFS_NAME_DIR = "dfs.name.dir";
    public static final String DFS_NAME_DIR_DEFAULT = "tmp/hadoop/dfs/name";

    public static final String FS_DEFAULT_NAME = "fs.default.name";
    public static final String FS_DEFAULT_NAME_DEFAULT = "localhost:8020";

    public static final String DFS_NAMENODE_HANDLER_COUNT = "dfs.namenode.handler.count";
    public static final int DFS_NAMENODE_HANDLER_COUNT_DEFAULT = 10;

}
