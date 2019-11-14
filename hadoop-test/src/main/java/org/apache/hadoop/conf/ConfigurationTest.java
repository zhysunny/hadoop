package org.apache.hadoop.conf;

import static org.junit.Assert.*;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.constant.FilePathConstant;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.AbstractMapWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.log4j.xml.DOMConfigurator;
import org.junit.*;

import java.io.File;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.util.*;

/**
 * Configuration Test
 * @author 章云
 * @date 2019/6/22 10:21
 */
public class ConfigurationTest {

    private static Configuration conf;
    private static final File OUTPUT_XML = new File("output.xml");
    private static final File OUTPUT_JSON = new File("output.json");

    @BeforeClass
    public static void beforeClass() throws Exception {
        System.out.println("Test Configuration Class Start...");
        FilePathConstant.init(FilePathConstant.JUNIT);
        DOMConfigurator.configure(FilePathConstant.LOG4J_TEST_XML);
    }

    @Before
    public void before() throws Exception {
        conf = new Configuration();
    }

    @After
    public void after() throws Exception {
    }

    @AfterClass
    public static void afterClass() throws Exception {
        FileUtils.forceDelete(OUTPUT_XML);
        FileUtils.forceDelete(OUTPUT_JSON);
        System.out.println("Test Configuration Class End...");
    }

    /**
     * Method: getProps()
     */
    @Test
    public void testGetProps() throws Exception {
        int size = conf.getProps().size();
        assertTrue("加载默认配置文件失败", size > 0);
        for (Map.Entry<Object, Object> entry : conf.getProps().entrySet()) {
            System.out.println(entry.getKey() + "=" + conf.get(entry.getKey().toString()));
        }
    }

    /**
     * Method: addResource(String name)
     */
    @Test
    public void testAddResourceName() throws Exception {
        conf.reloadConfiguration();
        int size = conf.getProps().size();
        assertTrue("配置清空失败", size == 0);
        conf.addResource(FilePathConstant.HDFS_DEFAULT_XML);
        size = conf.getProps().size();
        assertTrue("加载配置文件失败", size > 0);
        for (Map.Entry<Object, Object> entry : conf.getProps().entrySet()) {
            System.out.println(entry.getKey() + "=" + conf.get(entry.getKey().toString()));
        }
    }

    /**
     * Method: addResource(Path file)
     */
    @Test
    public void testAddResourceFile() throws Exception {
        conf.reloadConfiguration();
        int size = conf.getProps().size();
        assertTrue("配置清空失败", size == 0);
        conf.addResource(new Path(FilePathConstant.HDFS_DEFAULT_XML));
        size = conf.getProps().size();
        assertTrue("加载配置文件失败", size > 0);
        for (Map.Entry<Object, Object> entry : conf.getProps().entrySet()) {
            System.out.println(entry.getKey() + "=" + conf.get(entry.getKey().toString()));
        }
    }

    /**
     * Method: get(String name)
     */
    @Test
    public void testGetName() throws Exception {
        for (Map.Entry<Object, Object> entry : conf.getProps().entrySet()) {
            if (conf.getRaw(entry.getKey().toString()).contains("${")) {
                //原始值包含${表示可翻译
                System.out.println(entry.getKey() + "=" + conf.getRaw(entry.getKey().toString()));
                String raw = conf.getRaw(entry.getKey().toString());
                String value = conf.get(entry.getKey().toString());
                assertFalse(raw + "==" + value + ",配置翻译失败", raw.equals(value));
            }
        }
    }

    /**
     * Method: set(String name, String value)
     */
    @Test
    public void testSet() throws Exception {
        conf.set("test.config3", "${test.config1}config3");
        //这时配置中没有test.config1，所以没法翻译
        System.out.println(conf.get("test.config3"));
        conf.set("test.config1", "config1");
        conf.set("test.config2", "${test.config1}config2");
        System.out.println(conf.get("test.config1"));
        System.out.println(conf.getRaw("test.config1"));
        System.out.println(conf.get("test.config2"));
        System.out.println(conf.getRaw("test.config2"));
        System.out.println(conf.get("test.config3"));
        System.out.println(conf.getRaw("test.config3"));
    }

    /**
     * Method: unset(String name)
     */
    @Test
    public void testUnset() throws Exception {
        conf.set("test.config1", "config1");
        //删除配置项
        conf.unset("test.config1");
        assertFalse(conf.getProps().containsKey("test.config1"));
    }

    /**
     * Method: setIfUnset(String name, String value)
     */
    @Test
    public void testSetIfUnset() throws Exception {
        //当配置项不存在或为null时才增加配置
        //测试增加同一个配置项
        conf.set("test.config1", "config1");
        conf.setIfUnset("test.config1", "config2");
        assertTrue(conf.get("test.config1").equals("config1"));
        //测试增加不同配置项
        conf.setIfUnset("test.config2", "config1");
        assertTrue(conf.get("test.config1").equals(conf.get("test.config2")));
    }

    /**
     * Method: get(String name, String defaultValue)
     */
    @Test
    public void testGetForNameDefaultValue() throws Exception {
        String value = conf.get("test.config", "config");
        //test.config配置不存在，这里使用默认值
        assertTrue("config".equals(value));
        value = conf.get("fs.default.name", "config");
        //test.config配置存在，不会使用默认值
        assertFalse("config".equals(value));
    }

    /**
     * Method: getInt(String name, int defaultValue)
     */
    @Test
    public void testGetInt() throws Exception {
        conf.set("test.config", "123");
        int value = conf.getInt("test.config", 0);
        assertTrue(value == 123);
        //16进制
        conf.set("test.config", "0x123");
        value = conf.getInt("test.config", 0);
        assertFalse(value == 123);
        System.out.println(value);
        //如果不是数字，返回设置的默认值
        conf.set("test.config", "xxxxx");
        value = conf.getInt("test.config", 0);
        assertTrue(value == 0);
    }

    /**
     * Method: getLong(String name, long defaultValue)
     */
    @Test
    public void testGetLong() throws Exception {
        conf.set("test.config", "123");
        long value = conf.getLong("test.config", 0L);
        assertTrue(value == 123);
        //16进制
        conf.set("test.config", "0x123");
        value = conf.getLong("test.config", 0L);
        assertFalse(value == 123);
        System.out.println(value);
        //如果不是数字，返回设置的默认值
        conf.set("test.config", "xxxxx");
        value = conf.getLong("test.config", 0L);
        assertTrue(value == 0);
    }

    /**
     * Method: getFloat(String name, float defaultValue)
     */
    @Test
    public void testGetFloat() throws Exception {
        conf.set("test.config", "123");
        float value = conf.getFloat("test.config", 0L);
        System.out.println(value);
        assertTrue(value == 123);
        //如果不是数字，返回设置的默认值
        conf.set("test.config", "xxxxx");
        value = conf.getLong("test.config", 0L);
        assertTrue(value == 0);
    }

    /**
     * Method: getRange(String name, String defaultValue)
     */
    @Test
    public void testGetRange() throws Exception {
        // 获得一个数字范围列表
        Configuration.IntegerRanges range = conf.getRange("test.config", "1,3-8,9-11");
        System.out.println(range);
        assertTrue(range.isIncluded(5));
    }

    /**
     * Method: getStringCollection(String name)
     */
    @Test
    public void testGetStringCollection() throws Exception {
        //配置值按逗号分隔,返回ArrayList
        conf.set("test.config", "1,2,3,4,5");
        Collection<String> result = conf.getStringCollection("test.config");
        assertTrue(result.size() == 5);
        assertTrue(result.getClass() == ArrayList.class);
    }

    /**
     * Method: getStrings(String name)
     */
    @Test
    public void testGetStrings() throws Exception {
        //测试getStrings重载方法
        conf.set("test.config", "1,2,3,4,5");
        String[] result = conf.getStrings("test.config");
        assertTrue(result.length == 5);
        assertTrue(result.getClass() == String[].class);
        conf.unset("test.config");
        //此时defaultValue就是数组，不会再对元素分割，set方法也不会二次分割
        result = conf.getStrings("test.config", "1,2,3,4,5", "1,2,3,4,5");
        Arrays.stream(result).forEach(System.out::println);
        assertTrue(result.length == 2);
    }

    /**
     * Method: getClass(String name, Class<? extends U> defaultValue, Class<U> xface)
     */
    @Test
    public void testGetClassForNameDefaultValueXface() throws Exception {
        //Configurable是接口
        Class<Configurable> configurationClass = Configurable.class;
        //AbstractMapWritable实现Configurable的抽象类
        Class<AbstractMapWritable> abstractMapWritableClass = AbstractMapWritable.class;
        //MapWritable继承AbstractMapWritable
        Class<MapWritable> mapWritableClass = MapWritable.class;
        assertTrue(configurationClass.isAssignableFrom(abstractMapWritableClass));
        assertTrue(configurationClass.isAssignableFrom(mapWritableClass));
        assertTrue(abstractMapWritableClass.isAssignableFrom(mapWritableClass));
        assertFalse(abstractMapWritableClass.isAssignableFrom(configurationClass));
        // asSubclass方法测试AbstractMapWritable是否为Configurable的子类，不是抛ClassCastException异常
        abstractMapWritableClass.asSubclass(configurationClass);
    }

    /**
     * Method: size()
     */
    @Test
    public void testSize() throws Exception {
        assertTrue(conf.size() > 0);
    }

    /**
     * Method: clear()
     */
    @Test
    public void testClear() throws Exception {
        conf.clear();
        assertTrue(conf.size() == 0);
    }

    /**
     * Method: iterator()
     */
    @Test
    public void testIterator() throws Exception {
        //properties的迭代器
        Iterator<Map.Entry<String, String>> iterator = conf.iterator();
        while (iterator.hasNext()) {
            Map.Entry<String, String> entry = iterator.next();
            System.out.println(entry.getKey() + "=" + conf.get(entry.getKey().toString()));
        }
    }

    /**
     * Method: writeXml(OutputStream out)
     */
    @Test
    public void testWriteXmlOut() throws Exception {
        FileOutputStream fos = new FileOutputStream(OUTPUT_XML);
        conf.writeXml(fos);
        fos.close();
    }

    /**
     * Method: dumpConfiguration(Configuration config, Writer out)
     */
    @Test
    public void testDumpConfiguration() throws Exception {
        FileWriter fw = new FileWriter(OUTPUT_JSON);
        Configuration.dumpConfiguration(conf, fw);
        fw.close();
    }

    /**
     * Method: getValByRegex(String regex)
     */
    @Test
    public void testGetValByRegex() throws Exception {
        Map<String, String> configs = conf.getValByRegex("hadoop");
        for (Map.Entry<String, String> entry : configs.entrySet()) {
            System.out.println(entry.getKey() + "=" + conf.get(entry.getKey()));
        }
    }

} 
