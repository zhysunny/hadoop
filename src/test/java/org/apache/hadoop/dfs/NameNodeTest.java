package org.apache.hadoop.dfs;

import org.junit.*;

/**
 * NameNode Test.
 * @author 章云
 * @date 2019/7/30 16:28
 */
public class NameNodeTest {

    @BeforeClass
    public static void beforeClass() throws Exception {
        System.out.println("Test NameNode Class Start...");
    }

    @Before
    public void before() throws Exception {
    }

    @After
    public void after() throws Exception {
    }

    @AfterClass
    public static void afterClass() throws Exception {
        System.out.println("Test NameNode Class End...");
    }

    /**
     * Method: open(String src)
     */
    @Test
    public void testOpen() throws Exception {
//TODO: Test goes here... 
    }

    /**
     * Method: create(String src, String clientName, String clientMachine, boolean overwrite)
     */
    @Test
    public void testCreate() throws Exception {
//TODO: Test goes here... 
    }

    /**
     * Method: addBlock(String src, String clientMachine)
     */
    @Test
    public void testAddBlock() throws Exception {
//TODO: Test goes here... 
    }

    /**
     * Method: reportWrittenBlock(LocatedBlock lb)
     */
    @Test
    public void testReportWrittenBlock() throws Exception {
//TODO: Test goes here... 
    }

    /**
     * Method: abandonBlock(Block b, String src)
     */
    @Test
    public void testAbandonBlock() throws Exception {
//TODO: Test goes here... 
    }

    /**
     * Method: abandonFileInProgress(String src)
     */
    @Test
    public void testAbandonFileInProgress() throws Exception {
//TODO: Test goes here... 
    }

    /**
     * Method: complete(String src, String clientName)
     */
    @Test
    public void testComplete() throws Exception {
//TODO: Test goes here... 
    }

    /**
     * Method: getHints(String src, long start, long len)
     */
    @Test
    public void testGetHints() throws Exception {
//TODO: Test goes here... 
    }

    /**
     * Method: rename(String src, String dst)
     */
    @Test
    public void testRename() throws Exception {
//TODO: Test goes here... 
    }

    /**
     * Method: delete(String src)
     */
    @Test
    public void testDelete() throws Exception {
//TODO: Test goes here... 
    }

    /**
     * Method: exists(String src)
     */
    @Test
    public void testExists() throws Exception {
//TODO: Test goes here... 
    }

    /**
     * Method: isDir(String src)
     */
    @Test
    public void testIsDir() throws Exception {
//TODO: Test goes here... 
    }

    /**
     * Method: mkdirs(String src)
     */
    @Test
    public void testMkdirs() throws Exception {
//TODO: Test goes here... 
    }

    /**
     * Method: obtainLock(String src, String clientName, boolean exclusive)
     */
    @Test
    public void testObtainLock() throws Exception {
//TODO: Test goes here... 
    }

    /**
     * Method: releaseLock(String src, String clientName)
     */
    @Test
    public void testReleaseLock() throws Exception {
//TODO: Test goes here... 
    }

    /**
     * Method: renewLease(String clientName)
     */
    @Test
    public void testRenewLease() throws Exception {
//TODO: Test goes here... 
    }

    /**
     * Method: getListing(String src)
     */
    @Test
    public void testGetListing() throws Exception {
//TODO: Test goes here... 
    }

    /**
     * Method: getStats()
     */
    @Test
    public void testGetStats() throws Exception {
//TODO: Test goes here... 
    }

    /**
     * Method: getDatanodeReport()
     */
    @Test
    public void testGetDatanodeReport() throws Exception {
//TODO: Test goes here... 
    }

    /**
     * Method: sendHeartbeat(String sender, long capacity, long remaining)
     */
    @Test
    public void testSendHeartbeat() throws Exception {
//TODO: Test goes here... 
    }

    /**
     * Method: blockReport(String sender, Block blocks[])
     */
    @Test
    public void testBlockReport() throws Exception {
//TODO: Test goes here... 
    }

    /**
     * Method: blockReceived(String sender, Block blocks[])
     */
    @Test
    public void testBlockReceived() throws Exception {
//TODO: Test goes here... 
    }

    /**
     * Method: errorReport(String sender, String msg)
     */
    @Test
    public void testErrorReport() throws Exception {
//TODO: Test goes here... 
    }

    /**
     * Method: getBlockwork(String sender, int xmitsInProgress)
     */
    @Test
    public void testGetBlockwork() throws Exception {
//TODO: Test goes here... 
    }

    /**
     * Method: main(String argv[])
     */
    @Test
    public void testFormat() throws Exception {
        String[] args = new String[]{"-format"};
        NameNode.main(args);
    }

    /**
     * Method: main(String argv[])
     */
    @Test
    public void testMain() throws Exception {
        String[] args = new String[0];
        NameNode.main(args);
    }

}
