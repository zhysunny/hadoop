package org.apache.hadoop.util;

import org.apache.hadoop.conf.Configuration;

/**
 * 常量类
 * @author 章云
 * @date 2019/8/11 21:38
 */
public class Constants {

    private static final Configuration CONF = new Configuration();

    /*********************************** i/o properties ***********************************************/
    public static final int IO_SORT_FACTOR = CONF.getInt("io.sort.factor", 10);
    public static final int IO_SORT_MB = CONF.getInt("io.sort.mb", 100);
    public static final int IO_FILE_BUFFER_SIZE = CONF.getInt("io.file.buffer.size", 4096);
    public static final int IO_BYTES_PER_CHECKSUM = CONF.getInt("io.bytes.per.checksum", 512);
    public static final boolean IO_SKIP_CHECKSUM_ERRORS = CONF.getBoolean("io.skip.checksum.errors", false);
    public static final int IO_MAP_INDEX_SKIP = CONF.getInt("io.map.index.skip", 0);

    /*********************************** file system properties ***********************************************/
    public static final String FS_DEFAULT_NAME = CONF.get("fs.default.name", "local");
    public static final String DFS_NAME_DIR = CONF.get("dfs.name.dir", "tmp/hadoop/dfs/name");
    public static final String DFS_DATA_DIR = CONF.get("dfs.data.dir", "tmp/hadoop/dfs/data");
    public static final int DFS_DATANODE_PORT = CONF.getInt("dfs.datanode.port", 50010);
    public static final int DFS_REPLICATION = CONF.getInt("dfs.replication", 3);
    public static final int DFS_DF_INTERVAL = CONF.getInt("dfs.df.interval", 3000);

    /*********************************** map/reduce properties ***********************************************/
    public static final String MAPRED_JOB_TRACKER = CONF.get("mapred.job.tracker", "local");
    public static final int MAPRED_JOB_TRACKER_INFO_PORT = CONF.getInt("mapred.job.tracker.info.port", 50030);
    public static final int MAPRED_TASK_TRACKER_OUTPUT_PORT = CONF.getInt("mapred.task.tracker.output.port", 50040);
    public static final int MAPRED_TASK_TRACKER_REPORT_PORT = CONF.getInt("mapred.task.tracker.report.port", 50050);
    public static final String MAPRED_LOCAL_DIR = CONF.get("mapred.local.dir", "tmp/hadoop/mapred/local");
    public static final String MAPRED_SYSTEM_DIR = CONF.get("mapred.system.dir", "tmp/hadoop/mapred/system");
    public static final String MAPRED_TEMP_DIR = CONF.get("mapred.temp.dir", "tmp/hadoop/mapred/temp");
    public static final int MAPRED_MAP_TASKS = CONF.getInt("mapred.map.tasks", 2);
    public static final int MAPRED_REDUCE_TASKS = CONF.getInt("mapred.reduce.tasks", 1);
    public static final int MAPRED_TASK_TIMEOUT = CONF.getInt("mapred.task.timeout", 600000);
    public static final int MAPRED_TASKTRACKER_TASKS_MAXIMUM = CONF.getInt("mapred.tasktracker.tasks.maximum", 2);
    public static final String MAPRED_CHILD_JAVA_OPTS = CONF.get("mapred.child.java.opts", "-Xmx200m");
    public static final int MAPRED_COMBINE_BUFFER_SIZE = CONF.getInt("mapred.combine.buffer.size", 100000);
    public static final boolean MAPRED_SPECULATIVE_EXECUTION = CONF.getBoolean("mapred.speculative.execution", true);
    public static final int MAPRED_MIN_SPLIT_SIZE = CONF.getInt("mapred.min.split.size", 0);

    /*********************************** ipc properties ***********************************************/
    public static final int IPC_CLIENT_TIMEOUT = CONF.getInt("ipc.client.timeout", 60000);

    /*********************************** other properties ***********************************************/
    public static final int DFS_NAMENODE_HANDLER_COUNT = CONF.getInt("dfs.namenode.handler.count", 10);
    public static final int DFS_MAX_REPL_STREAMS = CONF.getInt("dfs.max-repl-streams", 2);
    public static final long FS_LOCAL_BLOCK_SIZE = CONF.getLong("fs.local.block.size", 1L << 25);
    public static final long DFS_BLOCKREPORT_INTERVALMSEC = CONF.getLong("dfs.blockreport.intervalMsec", 60 * 60 * 1000);
    public static final long DFS_DATANODE_STARTUPMSEC = CONF.getLong("dfs.datanode.startupMsec", 2 * 60 * 1000);

}
