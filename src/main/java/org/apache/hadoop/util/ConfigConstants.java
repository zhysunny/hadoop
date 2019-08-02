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
    public static final String FS_DEFAULT_NAME_DEFAULT = "local";

    public static final String DFS_NAME_DIR = "dfs.name.dir";
    public static final String DFS_NAME_DIR_DEFAULT = "tmp/hadoop/dfs/name";

    public static final String DFS_DATA_DIR = "dfs.data.dir";
    public static final String DFS_DATA_DIR_DEFAULT = "tmp/hadoop/dfs/data";

    public static final String DFS_DATANODE_PORT = "dfs.datanode.port";
    public static final int DFS_DATANODE_PORT_DEFAULT = 50010;

    public static final String DFS_REPLICATION = "dfs.replication";
    public static final int DFS_REPLICATION_DEFAULT = 3;

    public static final String DFS_DF_INTERVAL = "dfs.df.interval";
    public static final int DFS_DF_INTERVAL_DEFAULT = 3000;

    /*********************************** map/reduce properties ***********************************************/

    public static final String MAPRED_JOB_TRACKER = "mapred.job.tracker";
    public static final String MAPRED_JOB_TRACKER_DEFAULT = "local";

    public static final String MAPRED_JOB_TRACKER_INFO_PORT = "mapred.job.tracker.info.port";
    public static final int MAPRED_JOB_TRACKER_INFO_PORT_DEFAULT = 50030;

    public static final String MAPRED_TASK_TRACKER_OUTPUT_PORT = "mapred.task.tracker.output.port";
    public static final int MAPRED_TASK_TRACKER_OUTPUT_PORT_DEFAULT = 50040;

    public static final String MAPRED_TASK_TRACKER_REPORT_PORT = "mapred.task.tracker.report.port";
    public static final int MAPRED_TASK_TRACKER_REPORT_PORT_DEFAULT = 50050;

    public static final String MAPRED_LOCAL_DIR = "mapred.local.dir";
    public static final String MAPRED_LOCAL_DIR_DEFAULT = "tmp/hadoop/mapred/local";

    public static final String MAPRED_SYSTEM_DIR = "mapred.system.dir";
    public static final String MAPRED_SYSTEM_DIR_DEFAULT = "tmp/hadoop/mapred/system";

    public static final String MAPRED_TEMP_DIR = "mapred.temp.dir";
    public static final String MAPRED_TEMP_DIR_DEFAULT = "tmp/hadoop/mapred/temp";

    public static final String MAPRED_MAP_TASKS = "mapred.map.tasks";
    public static final int MAPRED_MAP_TASKS_DEFAULT = 2;

    public static final String MAPRED_REDUCE_TASKS = "mapred.reduce.tasks";
    public static final int MAPRED_REDUCE_TASKS_DEFAULT = 1;

    public static final String MAPRED_TASK_TIMEOUT = "mapred.task.timeout";
    public static final int MAPRED_TASK_TIMEOUT_DEFAULT = 600000;

    public static final String MAPRED_TASKTRACKER_TASKS_MAXIMUM = "mapred.tasktracker.tasks.maximum";
    public static final int MAPRED_TASKTRACKER_TASKS_MAXIMUM_DEFAULT = 2;

    public static final String MAPRED_CHILD_JAVA_OPTS = "mapred.child.java.opts";
    public static final String MAPRED_CHILD_JAVA_OPTS_DEFAULT = "-Xmx200m";

    public static final String MAPRED_COMBINE_BUFFER_SIZE = "mapred.combine.buffer.size";
    public static final int MAPRED_COMBINE_BUFFER_SIZE_DEFAULT = 100000;

    public static final String MAPRED_SPECULATIVE_EXECUTION = "mapred.speculative.execution";
    public static final boolean MAPRED_SPECULATIVE_EXECUTION_DEFAULT = true;

    public static final String MAPRED_MIN_SPLIT_SIZE = "mapred.min.split.size";
    public static final int MAPRED_MIN_SPLIT_SIZE_DEFAULT = 0;

    /*********************************** ipc properties ***********************************************/

    public static final String IPC_CLIENT_TIMEOUT = "ipc.client.timeout";
    public static final int IPC_CLIENT_TIMEOUT_DEFAULT = 60000;

    /*********************************** other properties ***********************************************/

    public static final String DFS_NAMENODE_HANDLER_COUNT = "dfs.namenode.handler.count";
    public static final int DFS_NAMENODE_HANDLER_COUNT_DEFAULT = 10;

    public static final String DFS_MAX_REPL_STREAMS = "dfs.max-repl-streams";
    public static final int DFS_MAX_REPL_STREAMS_DEFAULT = 2;

}
