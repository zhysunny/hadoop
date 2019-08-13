package org.apache.hadoop.dfs;

import static org.junit.Assert.*;
import static org.junit.Assert.assertEquals;

import org.junit.*;

import java.io.*;

/**
 * Block Test.
 * @author 章云
 * @date 2019/8/13 19:54
 */
public class BlockTest {

    private static File path;

    @BeforeClass
    public static void beforeClass() throws Exception {
        System.out.println("Test Block Class Start...");
        path = new File("junit/Block");
        path.mkdirs();
    }

    @Before
    public void before() throws Exception {
    }

    @After
    public void after() throws Exception {
    }

    @AfterClass
    public static void afterClass() throws Exception {
        System.out.println("Test Block Class End...");
    }

    @Test
    public void testBlock() throws Exception {
        // 随机一个block
        Block block = new Block();
        System.out.println("getBlockId:" + block.getBlockId());
        System.out.println("getBlockName:" + block.getBlockName());
        System.out.println("getNumBytes:" + block.getNumBytes());
        System.out.println("toString:" + block.toString());
        // 将block信息保存在对应的block文件中
        File blockFile = new File(path, block.getBlockName());
        DataOutputStream dos = new DataOutputStream(new FileOutputStream(blockFile));
        block.write(dos);
        // 新随机一个block
        Block newBlock = new Block();
        System.out.println(newBlock.getBlockName());
        // 新的block读取就block文件信息
        DataInputStream dis = new DataInputStream(new FileInputStream(blockFile));
        newBlock.readFields(dis);
        System.out.println(newBlock.getBlockName());
        // 最后两个block一样
        assertEquals(newBlock, block);
    }

}
