package org.apache.hadoop.util;

/**
 * 配置文件常量类
 * @author 章云
 * @date 2019/6/20 11:34
 */
public class FilePathConstant {
    private static String CONF_DIR = "conf/";

    /**
     * hadoop默认配置资源
     */
    public static String HADOOP_DEFAULT_XML = CONF_DIR.concat("hadoop-default.xml");
    /**
     * hadoop最终配置资源
     */
    public static String HADOOP_SITE_XML = CONF_DIR.concat("hadoop-site.xml");

}
