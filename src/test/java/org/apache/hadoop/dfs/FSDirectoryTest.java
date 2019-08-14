package org.apache.hadoop.dfs;

import static org.junit.Assert.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.Constants;
import org.junit.*;

import java.io.File;
import java.io.IOException;
import java.util.Map;

/**
 * FSDirectory Test.
 * @author 章云
 * @date 2019/8/13 21:21
 */
public class FSDirectoryTest {

    @BeforeClass
    public static void beforeClass() throws Exception {
        System.out.println("Test FSDirectory Class Start...");
    }

    @Before
    public void before() throws Exception {
    }

    @After
    public void after() throws Exception {
    }

    @AfterClass
    public static void afterClass() throws Exception {
        System.out.println("Test FSDirectory Class End...");
    }

    @Test
    public void testFSDirectory() throws Exception {
        FSDirectory fsDirectory = new FSDirectory(new File(Constants.DFS_NAME_DIR));
        FSDirectory.INode rootDir = fsDirectory.rootDir;
        assertNull(rootDir.parent);
        assertNull(rootDir.blocks);
        assertEquals("", rootDir.name);
        System.out.println(rootDir);
    }

} 
