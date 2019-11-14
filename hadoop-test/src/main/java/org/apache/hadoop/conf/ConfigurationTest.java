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
 * @author ����
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
        assertTrue("����Ĭ�������ļ�ʧ��", size > 0);
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
        assertTrue("�������ʧ��", size == 0);
        conf.addResource(FilePathConstant.HDFS_DEFAULT_XML);
        size = conf.getProps().size();
        assertTrue("���������ļ�ʧ��", size > 0);
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
        assertTrue("�������ʧ��", size == 0);
        conf.addResource(new Path(FilePathConstant.HDFS_DEFAULT_XML));
        size = conf.getProps().size();
        assertTrue("���������ļ�ʧ��", size > 0);
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
                //ԭʼֵ����${��ʾ�ɷ���
                System.out.println(entry.getKey() + "=" + conf.getRaw(entry.getKey().toString()));
                String raw = conf.getRaw(entry.getKey().toString());
                String value = conf.get(entry.getKey().toString());
                assertFalse(raw + "==" + value + ",���÷���ʧ��", raw.equals(value));
            }
        }
    }

    /**
     * Method: set(String name, String value)
     */
    @Test
    public void testSet() throws Exception {
        conf.set("test.config3", "${test.config1}config3");
        //��ʱ������û��test.config1������û������
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
        //ɾ��������
        conf.unset("test.config1");
        assertFalse(conf.getProps().containsKey("test.config1"));
    }

    /**
     * Method: setIfUnset(String name, String value)
     */
    @Test
    public void testSetIfUnset() throws Exception {
        //����������ڻ�Ϊnullʱ����������
        //��������ͬһ��������
        conf.set("test.config1", "config1");
        conf.setIfUnset("test.config1", "config2");
        assertTrue(conf.get("test.config1").equals("config1"));
        //�������Ӳ�ͬ������
        conf.setIfUnset("test.config2", "config1");
        assertTrue(conf.get("test.config1").equals(conf.get("test.config2")));
    }

    /**
     * Method: get(String name, String defaultValue)
     */
    @Test
    public void testGetForNameDefaultValue() throws Exception {
        String value = conf.get("test.config", "config");
        //test.config���ò����ڣ�����ʹ��Ĭ��ֵ
        assertTrue("config".equals(value));
        value = conf.get("fs.default.name", "config");
        //test.config���ô��ڣ�����ʹ��Ĭ��ֵ
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
        //16����
        conf.set("test.config", "0x123");
        value = conf.getInt("test.config", 0);
        assertFalse(value == 123);
        System.out.println(value);
        //����������֣��������õ�Ĭ��ֵ
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
        //16����
        conf.set("test.config", "0x123");
        value = conf.getLong("test.config", 0L);
        assertFalse(value == 123);
        System.out.println(value);
        //����������֣��������õ�Ĭ��ֵ
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
        //����������֣��������õ�Ĭ��ֵ
        conf.set("test.config", "xxxxx");
        value = conf.getLong("test.config", 0L);
        assertTrue(value == 0);
    }

    /**
     * Method: getRange(String name, String defaultValue)
     */
    @Test
    public void testGetRange() throws Exception {
        // ���һ�����ַ�Χ�б�
        Configuration.IntegerRanges range = conf.getRange("test.config", "1,3-8,9-11");
        System.out.println(range);
        assertTrue(range.isIncluded(5));
    }

    /**
     * Method: getStringCollection(String name)
     */
    @Test
    public void testGetStringCollection() throws Exception {
        //����ֵ�����ŷָ�,����ArrayList
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
        //����getStrings���ط���
        conf.set("test.config", "1,2,3,4,5");
        String[] result = conf.getStrings("test.config");
        assertTrue(result.length == 5);
        assertTrue(result.getClass() == String[].class);
        conf.unset("test.config");
        //��ʱdefaultValue�������飬�����ٶ�Ԫ�طָset����Ҳ������ηָ�
        result = conf.getStrings("test.config", "1,2,3,4,5", "1,2,3,4,5");
        Arrays.stream(result).forEach(System.out::println);
        assertTrue(result.length == 2);
    }

    /**
     * Method: getClass(String name, Class<? extends U> defaultValue, Class<U> xface)
     */
    @Test
    public void testGetClassForNameDefaultValueXface() throws Exception {
        //Configurable�ǽӿ�
        Class<Configurable> configurationClass = Configurable.class;
        //AbstractMapWritableʵ��Configurable�ĳ�����
        Class<AbstractMapWritable> abstractMapWritableClass = AbstractMapWritable.class;
        //MapWritable�̳�AbstractMapWritable
        Class<MapWritable> mapWritableClass = MapWritable.class;
        assertTrue(configurationClass.isAssignableFrom(abstractMapWritableClass));
        assertTrue(configurationClass.isAssignableFrom(mapWritableClass));
        assertTrue(abstractMapWritableClass.isAssignableFrom(mapWritableClass));
        assertFalse(abstractMapWritableClass.isAssignableFrom(configurationClass));
        // asSubclass��������AbstractMapWritable�Ƿ�ΪConfigurable�����࣬������ClassCastException�쳣
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
        //properties�ĵ�����
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
