package org.apache.hadoop.constant;

import java.lang.reflect.Field;

/**
 * 配置文件常量类
 * @author 章云
 * @date 2019/6/20 11:34
 */
public class FilePathConstant {
    private static String CONF_DIR = "conf/";
    private static String systemEnv = System.getProperty("os.name");
    private static final String WINDOWS = "windows";
    private static final String LINUX = "linux";
    public static String JUNIT = "../";

    static {
        if (systemEnv != null) {
            systemEnv = systemEnv.toLowerCase();
            if (systemEnv.startsWith(WINDOWS)) {
                CONF_DIR = "resources/".concat(CONF_DIR);
            }
        }
    }

    /**
     * 默认核心配置
     */
    public static String CORE_DEFAULT_XML = CONF_DIR.concat("core-default.xml");
    /**
     * 自定义核心配置
     */
    public static String CORE_SITE_XML = CONF_DIR.concat("core-site.xml");
    /**
     * 默认hdfs配置
     */
    public static String HDFS_DEFAULT_XML = CONF_DIR.concat("hdfs-default.xml");
    /**
     * 自定义hdfs配置
     */
    public static String HDFS_SITE_XML = CONF_DIR.concat("hdfs-site.xml");
    /**
     * 默认mapred配置
     */
    public static String MAPRED_DEFAULT_XML = CONF_DIR.concat("mapred-default.xml");
    /**
     * 自定义mapred配置
     */
    public static String MAPRED_SITE_XML = CONF_DIR.concat("mapred-site.xml");
    public static String LOG4J_PROPERTIES = CONF_DIR.concat("log4j.properties");
    /**
     * junit测试日志文件
     */
    public static String LOG4J_TEST_XML = CONF_DIR.concat("log4j-test.xml");
    /**
     * windows环境启动日志
     */
    public static String LOG4J_WINDOWS_XML = CONF_DIR.concat("log4j-windows.xml");
    /**
     * metrics监控统计配置
     */
    public static String HADOOP_METRICS2_PROPERTIES = CONF_DIR.concat("hadoop-metrics2.properties");

    private static int count = 0;

    public static void init(String prefix) {
        if (count > 0) {
            return;
        }
        Class<?> cls = FilePathConstant.class;
        Field[] fields = cls.getFields();
        Class<?> type = null;
        String fieldName = null;
        String fieldValue = null;
        try {
            for (Field field : fields) {
                type = field.getType();
                fieldName = field.getName();
                if (type != String.class) {
                    continue;
                }
                if (fieldName.equals("JUNIT")) {
                    continue;
                }
                fieldValue = field.get(fieldName).toString();
                field.set(fieldName, JUNIT.concat(fieldValue));
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        count++;
    }

    public static String getSystemEnv() {
        return systemEnv;
    }

}
