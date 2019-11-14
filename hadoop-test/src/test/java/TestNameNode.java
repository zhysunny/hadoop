import org.apache.commons.io.FileUtils;
import org.apache.hadoop.constant.FilePathConstant;
import org.apache.hadoop.hdfs.server.namenode.NameNode;
import org.apache.log4j.xml.DOMConfigurator;
import org.junit.*;

import java.io.File;

public class TestNameNode {

    @BeforeClass
    public static void beforeClass() throws Exception {
        System.out.println("Test FSImage Class Start...");
        FilePathConstant.init(FilePathConstant.JUNIT);
        DOMConfigurator.configure(FilePathConstant.LOG4J_TEST_XML);
    }

    @Before
    public void before() throws Exception {
    }

    @After
    public void after() throws Exception {
    }

    @AfterClass
    public static void afterClass() throws Exception {
        System.out.println("Test FSImage Class End...");
    }

    @Test
    public void testFromat() throws Exception {
        File path = new File("tmp");
        FileUtils.deleteDirectory(path);
        String[] param = new String[]{"-format"};
        // in_use.lock文件锁
        // current/fsimage
        // current/edits
        // image/fsimage
        // current/fstime
        // current/VERSION
        NameNode.main(param);
    }

    @Test
    public void testNameNode() throws Exception {
        String[] param = new String[0];
        NameNode.main(param);
    }
}
