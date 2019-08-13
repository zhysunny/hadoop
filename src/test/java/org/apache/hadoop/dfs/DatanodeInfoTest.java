package org.apache.hadoop.dfs;

import static org.junit.Assert.*;

import org.apache.hadoop.io.UTF8;
import org.junit.*;

import java.util.Arrays;

/**
 * DatanodeInfo Test.
 * @author 章云
 * @date 2019/8/13 20:25
 */
public class DatanodeInfoTest {

    @BeforeClass
    public static void beforeClass() throws Exception {
        System.out.println("Test DatanodeInfo Class Start...");
    }

    @Before
    public void before() throws Exception {
    }

    @After
    public void after() throws Exception {
    }

    @AfterClass
    public static void afterClass() throws Exception {
        System.out.println("Test DatanodeInfo Class End...");
    }

    @Test
    public void testDatanodeInfo() throws Exception {
        // 模拟获得节点信息
        DatanodeInfo datanodeInfo = new DatanodeInfo(new UTF8("localhost:50010"));
        datanodeInfo.addBlock(new Block());
        datanodeInfo.addBlock(new Block());
        datanodeInfo.addBlock(new Block());
        datanodeInfo.updateHeartbeat(555, 333);

        System.out.println("getName:" + datanodeInfo.getName());
        System.out.println("getHost:" + datanodeInfo.getHost());
        System.out.println("toString:" + datanodeInfo.toString());
        System.out.println("getCapacity:" + datanodeInfo.getCapacity());
        System.out.println("getRemaining:" + datanodeInfo.getRemaining());
        System.out.println("lastUpdate:" + datanodeInfo.lastUpdate());
        System.out.println("getBlocks:" + Arrays.toString(datanodeInfo.getBlocks()));
    }

}
