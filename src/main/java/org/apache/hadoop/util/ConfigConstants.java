package org.apache.hadoop.util;

/**
 * 配置常量类
 * @author 章云
 * @date 2019/7/30 15:30
 */
public class ConfigConstants {

    /*********************************** i/o properties ***********************************************/

    public static final String IO_SORT_FACTOR = "io.sort.factor";
    public static final int IO_SORT_FACTOR_DEFAULT = 10;

    public static final String IO_SORT_MB = "io.sort.mb";
    public static final int IO_SORT_MB_DEFAULT = 100;

    public static final String IO_FILE_BUFFER_SIZE = "io.file.buffer.size";
    public static final int IO_FILE_BUFFER_SIZE_DEFAULT = 4096;

    public static final String IO_BYTES_PER_CHECKSUM = "io.bytes.per.checksum";
    public static final int IO_BYTES_PER_CHECKSUM_DEFAULT = 512;

    public static final String IO_SKIP_CHECKSUM_ERRORS = "io.skip.checksum.errors";
    public static final boolean IO_SKIP_CHECKSUM_ERRORS_DEFAULT = false;

    public static final String IO_MAP_INDEX_SKIP = "io.map.index.skip";
    public static final int IO_MAP_INDEX_SKIP_DEFAULT = 0;

    /*********************************** file system properties ***********************************************/

    public static final String FS_DEFAULT_NAME = "fs.default.name";
    public static final String FS_DEFAULT_NAME_DEFAULT = "localhost:8020";

    public static final String DFS_NAME_DIR = "dfs.name.dir";
    public static final String DFS_NAME_DIR_DEFAULT = "tmp/hadoop/dfs/name";

    public static final String DFS_NAMENODE_HANDLER_COUNT = "dfs.namenode.handler.count";
    public static final int DFS_NAMENODE_HANDLER_COUNT_DEFAULT = 10;

}
