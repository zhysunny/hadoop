package org.apache.hadoop.dfs;

import static org.junit.Assert.*;

import org.apache.hadoop.io.UTF8;
import org.junit.*;

/**
 * DataNodeReport Test.
 * @author 章云
 * @date 2019/8/13 21:01
 */
public class DataNodeReportTest {

    @BeforeClass
    public static void beforeClass() throws Exception {
        System.out.println("Test DataNodeReport Class Start...");
    }

    @Before
    public void before() throws Exception {
    }

    @After
    public void after() throws Exception {
    }

    @AfterClass
    public static void afterClass() throws Exception {
        System.out.println("Test DataNodeReport Class End...");
    }

    @Test
    public void testDataNodeReport() {
        // 模拟获得节点信息
        DatanodeInfo datanodeInfo = new DatanodeInfo(new UTF8("localhost:50010"));
        datanodeInfo.addBlock(new Block());
        datanodeInfo.addBlock(new Block());
        datanodeInfo.addBlock(new Block());
        datanodeInfo.updateHeartbeat(555, 333);

        DataNodeReport dataNodeReport = datanodeInfo.copyToReport();
        System.out.println(dataNodeReport.toString());
    }

}
