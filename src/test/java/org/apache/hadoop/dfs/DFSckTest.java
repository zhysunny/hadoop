package org.apache.hadoop.dfs;

import static org.junit.Assert.*;

import org.junit.*;

/**
 * DFSck Test.
 * @author 章云
 * @date 2019/8/13 19:42
 */
public class DFSckTest {

    @BeforeClass
    public static void beforeClass() throws Exception {
        System.out.println("Test DFSck Class Start...");
    }

    @Before
    public void before() throws Exception {
    }

    @After
    public void after() throws Exception {
    }

    @AfterClass
    public static void afterClass() throws Exception {
        System.out.println("Test DFSck Class End...");
    }

    /**
     * Method: fsck(String path)
     */
    @Test
    public void testFsck() throws Exception {
//TODO: Test goes here... 
    }

    /**
     * Method: isHealthy()
     */
    @Test
    public void testIsHealthy() throws Exception {
//TODO: Test goes here... 
    }

    /**
     * Method: addMissing(String id, long size)
     */
    @Test
    public void testAddMissing() throws Exception {
//TODO: Test goes here... 
    }

    /**
     * Method: getMissingIds()
     */
    @Test
    public void testGetMissingIds() throws Exception {
//TODO: Test goes here... 
    }

    /**
     * Method: getMissingSize()
     */
    @Test
    public void testGetMissingSize() throws Exception {
//TODO: Test goes here... 
    }

    /**
     * Method: setMissingSize(long missingSize)
     */
    @Test
    public void testSetMissingSize() throws Exception {
//TODO: Test goes here... 
    }

    /**
     * Method: getOverReplicatedBlocks()
     */
    @Test
    public void testGetOverReplicatedBlocks() throws Exception {
//TODO: Test goes here... 
    }

    /**
     * Method: setOverReplicatedBlocks(long overReplicatedBlocks)
     */
    @Test
    public void testSetOverReplicatedBlocks() throws Exception {
//TODO: Test goes here... 
    }

    /**
     * Method: getReplicationFactor()
     */
    @Test
    public void testGetReplicationFactor() throws Exception {
//TODO: Test goes here... 
    }

    /**
     * Method: getUnderReplicatedBlocks()
     */
    @Test
    public void testGetUnderReplicatedBlocks() throws Exception {
//TODO: Test goes here... 
    }

    /**
     * Method: setUnderReplicatedBlocks(long underReplicatedBlocks)
     */
    @Test
    public void testSetUnderReplicatedBlocks() throws Exception {
//TODO: Test goes here... 
    }

    /**
     * Method: getTotalDirs()
     */
    @Test
    public void testGetTotalDirs() throws Exception {
//TODO: Test goes here... 
    }

    /**
     * Method: setTotalDirs(long totalDirs)
     */
    @Test
    public void testSetTotalDirs() throws Exception {
//TODO: Test goes here... 
    }

    /**
     * Method: getTotalFiles()
     */
    @Test
    public void testGetTotalFiles() throws Exception {
//TODO: Test goes here... 
    }

    /**
     * Method: setTotalFiles(long totalFiles)
     */
    @Test
    public void testSetTotalFiles() throws Exception {
//TODO: Test goes here... 
    }

    /**
     * Method: getTotalSize()
     */
    @Test
    public void testGetTotalSize() throws Exception {
//TODO: Test goes here... 
    }

    /**
     * Method: setTotalSize(long totalSize)
     */
    @Test
    public void testSetTotalSize() throws Exception {
//TODO: Test goes here... 
    }

    /**
     * Method: getReplication()
     */
    @Test
    public void testGetReplication() throws Exception {
//TODO: Test goes here... 
    }

    /**
     * Method: setReplication(int replication)
     */
    @Test
    public void testSetReplication() throws Exception {
//TODO: Test goes here... 
    }

    /**
     * Method: getTotalBlocks()
     */
    @Test
    public void testGetTotalBlocks() throws Exception {
//TODO: Test goes here... 
    }

    /**
     * Method: setTotalBlocks(long totalBlocks)
     */
    @Test
    public void testSetTotalBlocks() throws Exception {
//TODO: Test goes here... 
    }

    /**
     * Method: toString()
     */
    @Test
    public void testToString() throws Exception {
//TODO: Test goes here... 
    }

    /**
     * Method: getCorruptFiles()
     */
    @Test
    public void testGetCorruptFiles() throws Exception {
//TODO: Test goes here... 
    }

    /**
     * Method: setCorruptFiles(long corruptFiles)
     */
    @Test
    public void testSetCorruptFiles() throws Exception {
//TODO: Test goes here... 
    }

    /**
     * Method: main(String[] args)
     */
    @Test
    public void testMain() throws Exception {
//        String[] args = new String[]{"/data/test", "-files"};
//        String[] args = new String[]{"/data/test", "-move"};
//        String[] args = new String[]{"/data/test", "-delete"};
//        String[] args = new String[]{"/data/test", "-blocks"};
        String[] args = new String[]{"/data/test", "-locations"};
        DFSck.main(args);
    }

} 
