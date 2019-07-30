package org.apache.hadoop.conf;

import static org.junit.Assert.*;

import org.junit.*;

import java.util.Arrays;

/**
 * Configuration Test.
 * @author 章云
 * @date 2019/7/30 10:54
 */
public class ConfigurationTest {

    private static Configuration conf;

    @BeforeClass
    public static void beforeClass() throws Exception {
        System.out.println("Test Configuration Class Start...");
        conf = new Configuration();
    }

    @Before
    public void before() throws Exception {
    }

    @After
    public void after() throws Exception {
    }

    @AfterClass
    public static void afterClass() throws Exception {
        System.out.println("Test Configuration Class End...");
    }

    /**
     * Method: getStrings(String name)
     */
    @Test
    public void testGetStrings() throws Exception {
        conf.set("test", "java hadoop,spark\thive\nhbase");
        String[] tests = conf.getStrings("test");
        assertEquals(tests.length, 5);
        assertEquals(tests[3], "hive");
    }

    /**
     * Method: toString()
     */
    @Test
    public void testToString() throws Exception {
        System.out.println(conf.toString());
    }

} 
