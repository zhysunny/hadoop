package org.apache.hadoop.io;

import static org.junit.Assert.*;
import static org.junit.Assert.assertEquals;

import org.junit.*;

import java.util.Random;

/**
 * UTF8 Test.
 * @author 章云
 * @date 2019/8/1 14:36
 */
public class UTF8Test {

    private String string = "开始学习hadoop源码";

    @BeforeClass
    public static void beforeClass() throws Exception {
        System.out.println("Test UTF8 Class Start...");
    }

    @Before
    public void before() throws Exception {
    }

    @After
    public void after() throws Exception {
    }

    @AfterClass
    public static void afterClass() throws Exception {
        System.out.println("Test UTF8 Class End...");
    }

    /**
     * Method: getBytes()
     */
    @Test
    public void testGetBytes() throws Exception {
        System.out.println(new String(UTF8.getBytes(string), "UTF-8"));
        assertEquals(string, new String(UTF8.getBytes(string), "UTF-8"));
        System.out.println(new String(new UTF8(string).getBytes(), "UTF-8"));
        assertEquals(string, new String(new UTF8(string).getBytes(), "UTF-8"));
    }

    /**
     * Method: getLength()
     */
    @Test
    public void testGetLength() throws Exception {
        // 一般英文长度为1，中文汉字长度是3
        assertEquals(new UTF8(string).getLength(), 24);
        System.out.println(new UTF8(string).getLength());
    }

    /**
     * Method: readFields(DataInput in)
     */
    @Test
    public void testReadFields() throws Exception {
        DataOutputBuffer out = new DataOutputBuffer();
        DataInputBuffer in = new DataInputBuffer();
        String newString = "学习源码";
        // 生成一个DataInputBuffer
        out.reset();
        UTF8.writeString(out, newString);
        in.reset(out.getData(), out.getLength());
        // 测试readFields方法
        UTF8 utf8 = new UTF8(string);
        assertEquals(string, utf8.toString());
        System.out.println(utf8.toString());
        // 读取数据输入流，覆盖原utf8存储的字符串
        utf8.readFields(in);
        assertEquals(newString, utf8.toString());
        System.out.println(utf8.toString());
    }

    /**
     * Method: write(DataOutput out)
     */
    @Test
    public void testWrite() throws Exception {
        DataOutputBuffer out = new DataOutputBuffer();
        DataInputBuffer in = new DataInputBuffer();
        String newString = "学习源码";
        // 生成一个DataInputBuffer
        out.reset();
        UTF8.writeString(out, newString);
        // 调用write()并没有改变UTF8本身存储的字符串
        UTF8 utf8 = new UTF8(string);
        System.out.println(out.getLength());
        // 相当于把UTF8存储的字符串追加到out输出流中
        utf8.write(out);
        System.out.println(out.getLength());
        System.out.println(utf8.toString());
        in.reset(out.getData(), out.getLength());
        utf8.readFields(in);
        System.out.println(utf8.toString());
        utf8.readFields(in);
        System.out.println(utf8.toString());
    }

    /**
     * Method: readString(DataInput in)
     */
    @Test
    public void testReadString() throws Exception {
        DataOutputBuffer out = new DataOutputBuffer();
        DataInputBuffer in = new DataInputBuffer();
        UTF8 utf8 = new UTF8(string);

        utf8.write(out);
        in.reset(out.getData(), out.getLength());
        System.out.println(UTF8.readString(in));
    }

    /**
     * Method: writeString(DataOutput out, String string)
     */
    @Test
    public void testWriteString() throws Exception {
        DataOutputBuffer out = new DataOutputBuffer();
        DataInputBuffer in = new DataInputBuffer();
        UTF8.writeString(out, string);
        in.reset(out.getData(), out.getLength());
        System.out.println(UTF8.readString(in));
    }

}
