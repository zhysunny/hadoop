package org.apache.hadoop.dfs;

import static org.junit.Assert.*;

import org.junit.*;

/**
 * DFSShell Test
 * @author 章云
 * @date 2019/8/3 14:16
 */
public class DFSShellTest {

    @BeforeClass
    public static void beforeClass() throws Exception {
        System.out.println("Test DFSShell Class Start...");
    }

    @Before
    public void before() throws Exception {
    }

    @After
    public void after() throws Exception {
    }

    @AfterClass
    public static void afterClass() throws Exception {
        System.out.println("Test DFSShell Class End...");
    }

    /**
     * Method: copyFromLocal(File src, String dstf)
     */
    @Test
    public void testCopyFromLocal() throws Exception {
//TODO: Test goes here... 
    }

    /**
     * Method: moveFromLocal(File src, String dstf)
     */
    @Test
    public void testMoveFromLocal() throws Exception {
//TODO: Test goes here... 
    }

    /**
     * Method: copyToLocal(String srcf, File dst)
     */
    @Test
    public void testCopyToLocal() throws Exception {
//TODO: Test goes here... 
    }

    /**
     * Method: moveToLocal(String srcf, File dst)
     */
    @Test
    public void testMoveToLocal() throws Exception {
//TODO: Test goes here... 
    }

    /**
     * Method: cat(String srcf)
     */
    @Test
    public void testCat() throws Exception {
//TODO: Test goes here... 
    }

    /**
     * Method: ls(String src, boolean recursive)
     */
    @Test
    public void testLs() throws Exception {
//TODO: Test goes here... 
    }

    /**
     * Method: du(String src)
     */
    @Test
    public void testDu() throws Exception {
//TODO: Test goes here... 
    }

    /**
     * Method: mkdir(String src)
     */
    @Test
    public void testMkdir() throws Exception {
//TODO: Test goes here... 
    }

    /**
     * Method: rename(String srcf, String dstf)
     */
    @Test
    public void testRename() throws Exception {
//TODO: Test goes here... 
    }

    /**
     * Method: copy(String srcf, String dstf, Configuration conf)
     */
    @Test
    public void testCopy() throws Exception {
//TODO: Test goes here... 
    }

    /**
     * Method: delete(String srcf)
     */
    @Test
    public void testDelete() throws Exception {
//TODO: Test goes here... 
    }

    /**
     * Method: byteDesc(long len)
     */
    @Test
    public void testByteDesc() throws Exception {
//TODO: Test goes here... 
    }

    /**
     * Method: limitDecimal(double d, int placesAfterDecimal)
     */
    @Test
    public void testLimitDecimal() throws Exception {
//TODO: Test goes here... 
    }

    /**
     * Method: report()
     */
    @Test
    public void testReport() throws Exception {
//TODO: Test goes here... 
    }

    /**
     * Method: main(String argv[])
     */
    @Test
    public void testMain() throws Exception {
//        String[] args = new String[]{"-mkdir", "/data/test"};
        String[] args = new String[]{"-ls", "/data/test"};
//        String[] args = new String[]{"-put", "dir.xml", "/data/test"};
        DFSShell.main(args);
    }


} 
