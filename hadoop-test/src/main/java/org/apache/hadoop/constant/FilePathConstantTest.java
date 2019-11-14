package org.apache.hadoop.constant;

import static org.junit.Assert.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.ReflectClass;
import org.junit.*;

import java.util.Map;

/**
 * FilePathConstant Test
 * @author 章云
 * @date 2019/6/22 10:18
 */
public class FilePathConstantTest {

    @BeforeClass
    public static void beforeClass() throws Exception {
        System.out.println("Test FilePathConstant Class Start...");
    }

    @Before
    public void before() throws Exception {
    }

    @AfterClass
    public static void afterClass() throws Exception {
        System.out.println("Test FilePathConstant Class End...");
    }

    @After
    public void after() throws Exception {
    }

    @Test
    public void test() throws Exception {
        Map<String, Object> map = ReflectClass.reflect(FilePathConstant.class);
        System.out.println("当前操作系统环境：" + FilePathConstant.getSystemEnv());
        for (Map.Entry<String, Object> entry : map.entrySet()) {
            System.out.println(entry.getKey() + "=" + entry.getValue());
        }
    }

}
