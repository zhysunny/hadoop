package org.apache.hadoop.hdfs.server.namenode;

import static org.junit.Assert.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.server.common.Storage;
import org.junit.*;
import org.apache.hadoop.constant.FilePathConstant;
import org.apache.log4j.xml.DOMConfigurator;

import java.io.File;
import java.util.Collection;
import java.util.List;

/**
 * FSImage Test
 * @author 章云
 * @date 2019/6/22 10:43
 */
public class FSImageTest {

    private FSImage fsImage;

    @BeforeClass
    public static void beforeClass() throws Exception {
        System.out.println("Test FSImage Class Start...");
        FilePathConstant.init(FilePathConstant.JUNIT);
        DOMConfigurator.configure(FilePathConstant.LOG4J_TEST_XML);
    }

    @Before
    public void before() throws Exception {
        // namenode -format start
        Configuration conf = new Configuration();
        conf.addResource(FilePathConstant.HDFS_DEFAULT_XML);
        conf.addResource(FilePathConstant.HDFS_SITE_XML);
        Collection<File> dirsToFormat = FSNamesystem.getNamespaceDirs(conf);
        Collection<File> editDirsToFormat = FSNamesystem.getNamespaceEditsDirs(conf);
        fsImage = new FSImage(dirsToFormat, editDirsToFormat);
        // namenode -format end
    }

    @After
    public void after() throws Exception {
        fsImage = null;
    }

    @AfterClass
    public static void afterClass() throws Exception {
        System.out.println("Test FSImage Class End...");
    }

    /**
     * Method: setStorageDirectories(Collection<File> fsNameDirs, Collection<File> fsEditsDirs)
     */
    @Test
    public void testSetStorageDirectories() throws Exception {
        //fsImage = new FSImage(dirsToFormat, editDirsToFormat);已经执行过这个方法
        // 添加fsImage和edits存储目录，变量storageDirs
        int numStorageDirs = fsImage.getNumStorageDirs();
        assertFalse(numStorageDirs == 0);
        for (int i = 0; i < numStorageDirs; i++) {
            Storage.StorageDirectory storageDir = fsImage.getStorageDir(i);
            System.out.println(storageDir.getRoot());
            System.out.println(storageDir.getStorageDirType());
            System.out.println(storageDir.getCurrentDir());
            System.out.println(storageDir.getVersionFile());
            System.out.println(storageDir.getPreviousVersionFile());
            System.out.println(storageDir.getPreviousDir());
            System.out.println(storageDir.getPreviousTmp());
            System.out.println(storageDir.getRemovedTmp());
            System.out.println(storageDir.getFinalizedTmp());
            System.out.println(storageDir.getLastCheckpointTmp());
            System.out.println(storageDir.getPreviousCheckpoint());
        }
    }

    /**
     * Method: getImageFile(StorageDirectory sd, NameNodeFile type)
     */
    @Test
    public void testGetImageFile() throws Exception {
        FSImage.NameNodeFile[] values = FSImage.NameNodeFile.values();
        for (FSImage.NameNodeFile value : values) {
            System.out.println(FSImage.getImageFile(fsImage.getStorageDir(0), value).getAbsolutePath());
        }
    }

    /**
     * Method: getEditFile(StorageDirectory sd)
     */
    @Test
    public void testGetEditFile() throws Exception {
        System.out.println(fsImage.getEditFile(fsImage.getStorageDir(0)));
    }

    /**
     * Method: getEditNewFile(StorageDirectory sd)
     */
    @Test
    public void testGetEditNewFile() throws Exception {
        System.out.println(fsImage.getEditNewFile(fsImage.getStorageDir(0)));
    }

    /**
     * Method: getFileNames(NameNodeFile type, NameNodeDirType dirType)
     */
    @Test
    public void testGetFileNames() throws Exception {
        FSImage.NameNodeFile[] types = FSImage.NameNodeFile.values();
        for (FSImage.NameNodeFile type : types) {
            FSImage.NameNodeDirType[] dirTypes = FSImage.NameNodeDirType.values();
            for (FSImage.NameNodeDirType dirType : dirTypes) {
                File[] fileNames = fsImage.getFileNames(type, dirType);
                for (File file : fileNames) {
                    System.out.println(type);
                    System.out.println(dirType);
                    System.out.println(file.getAbsolutePath());
                    System.out.println("------------------");
                }
            }
        }
    }

    /**
     * Method: getImageFiles()
     */
    @Test
    public void testGetImageFiles() throws Exception {
        File[] fileNames = fsImage.getImageFiles();
        for (File file : fileNames) {
            System.out.println(file.getAbsolutePath());
        }
    }

    /**
     * Method: getEditsFiles()
     */
    @Test
    public void testGetEditsFiles() throws Exception {
        File[] fileNames = fsImage.getEditsFiles();
        for (File file : fileNames) {
            System.out.println(file.getAbsolutePath());
        }
    }

    /**
     * Method: recoverTransitionRead(Collection<File> dataDirs, Collection<File> editsDirs, StartupOption startOpt)
     */
    @Test
    public void testRecoverTransitionRead() throws Exception {
//TODO: Test goes here... 
    }

    /**
     * Method: doImportCheckpoint()
     */
    @Test
    public void testDoImportCheckpoint() throws Exception {
//TODO: Test goes here... 
    }

    /**
     * Method: finalizeUpgrade()
     */
    @Test
    public void testFinalizeUpgrade() throws Exception {
//TODO: Test goes here... 
    }

    /**
     * Method: isUpgradeFinalized()
     */
    @Test
    public void testIsUpgradeFinalized() throws Exception {
//TODO: Test goes here... 
    }

    /**
     * Method: getFields(Properties props, StorageDirectory sd)
     */
    @Test
    public void testGetFields() throws Exception {
//TODO: Test goes here... 
    }

    /**
     * Method: readCheckpointTime(StorageDirectory sd)
     */
    @Test
    public void testReadCheckpointTime() throws Exception {
//TODO: Test goes here... 
    }

    /**
     * Method: setFields(Properties props, StorageDirectory sd)
     */
    @Test
    public void testSetFields() throws Exception {
//TODO: Test goes here... 
    }

    /**
     * Method: writeCheckpointTime(StorageDirectory sd)
     */
    @Test
    public void testWriteCheckpointTime() throws Exception {
//TODO: Test goes here... 
    }

    /**
     * Method: incrementCheckpointTime()
     */
    @Test
    public void testIncrementCheckpointTime() throws Exception {
//TODO: Test goes here... 
    }

    /**
     * Method: removeStorageDir(File dir)
     */
    @Test
    public void testRemoveStorageDir() throws Exception {
//TODO: Test goes here... 
    }

    /**
     * Method: getEditLog()
     */
    @Test
    public void testGetEditLog() throws Exception {
//TODO: Test goes here... 
    }

    /**
     * Method: setEditLog(FSEditLog newLog)
     */
    @Test
    public void testSetEditLog() throws Exception {
//TODO: Test goes here... 
    }

    /**
     * Method: isConversionNeeded(StorageDirectory sd)
     */
    @Test
    public void testIsConversionNeeded() throws Exception {
//TODO: Test goes here... 
    }

    /**
     * Method: recoverInterruptedCheckpoint(StorageDirectory nameSD, StorageDirectory editsSD)
     */
    @Test
    public void testRecoverInterruptedCheckpoint() throws Exception {
//TODO: Test goes here... 
    }

    /**
     * Method: loadFSImage(MetaRecoveryContext recovery)
     */
    @Test
    public void testLoadFSImageRecovery() throws Exception {
//TODO: Test goes here... 
    }

    /**
     * Method: loadFSImage(File curFile)
     */
    @Test
    public void testLoadFSImageCurFile() throws Exception {
//TODO: Test goes here... 
    }

    /**
     * Method: getParent(String path)
     */
    @Test
    public void testGetParent() throws Exception {
//TODO: Test goes here... 
    }

    /**
     * Method: loadFSEdits(StorageDirectory sd, MetaRecoveryContext recovery)
     */
    @Test
    public void testLoadFSEdits() throws Exception {
//TODO: Test goes here... 
    }

    /**
     * Method: saveFSImage(File newFile)
     */
    @Test
    public void testSaveFSImage() throws Exception {
        fsImage.saveFSImage(new File("fsImage"));
    }

    /**
     * Method: saveNamespace(boolean renewCheckpointTime)
     */
    @Test
    public void testSaveNamespace() throws Exception {
//TODO: Test goes here... 
    }

    /**
     * Method: saveCurrent(StorageDirectory sd)
     */
    @Test
    public void testSaveCurrent() throws Exception {
//TODO: Test goes here... 
    }

    /**
     * Method: moveCurrent(StorageDirectory sd)
     */
    @Test
    public void testMoveCurrent() throws Exception {
//TODO: Test goes here... 
    }

    /**
     * Method: moveLastCheckpoint(StorageDirectory sd)
     */
    @Test
    public void testMoveLastCheckpoint() throws Exception {
//TODO: Test goes here... 
    }

    /**
     * Method: setRestoreRemovedDirs(boolean allow)
     */
    @Test
    public void testSetRestoreRemovedDirs() throws Exception {
//TODO: Test goes here... 
    }

    /**
     * Method: setEditsTolerationLength(int editsTolerationLength)
     */
    @Test
    public void testSetEditsTolerationLength() throws Exception {
//TODO: Test goes here... 
    }

    /**
     * Method: restoreStorageDirs()
     */
    @Test
    public void testRestoreStorageDirs() throws Exception {
//TODO: Test goes here... 
    }

    /**
     * Method: format(StorageDirectory sd)
     */
    @Test
    public void testFormatSd() throws Exception {
//TODO: Test goes here... 
    }

    /**
     * Method: format()
     */
    @Test
    public void testFormat() throws Exception {
        //TODO: Test goes here...
    }

    /**
     * Method: loadDatanodes(int version, DataInputStream in)
     */
    @Test
    public void testLoadDatanodes() throws Exception {
//TODO: Test goes here... 
    }

    /**
     * Method: readINodeUnderConstruction(DataInputStream in)
     */
    @Test
    public void testReadINodeUnderConstruction() throws Exception {
//TODO: Test goes here... 
    }

    /**
     * Method: writeINodeUnderConstruction(DataOutputStream out, INodeFileUnderConstruction cons, String path)
     */
    @Test
    public void testWriteINodeUnderConstruction() throws Exception {
//TODO: Test goes here... 
    }

    /**
     * Method: rollFSImage()
     */
    @Test
    public void testRollFSImage() throws Exception {
//TODO: Test goes here... 
    }

    /**
     * Method: rollEditLog()
     */
    @Test
    public void testRollEditLog() throws Exception {
//TODO: Test goes here... 
    }

    /**
     * Method: validateCheckpointUpload(CheckpointSignature sig)
     */
    @Test
    public void testValidateCheckpointUpload() throws Exception {
//TODO: Test goes here... 
    }

    /**
     * Method: checkpointUploadDone()
     */
    @Test
    public void testCheckpointUploadDone() throws Exception {
//TODO: Test goes here... 
    }

    /**
     * Method: close()
     */
    @Test
    public void testClose() throws Exception {
//TODO: Test goes here... 
    }

    /**
     * Method: getFsImageName()
     */
    @Test
    public void testGetFsImageName() throws Exception {
//TODO: Test goes here... 
    }

    /**
     * Method: getFsEditName()
     */
    @Test
    public void testGetFsEditName() throws Exception {
//TODO: Test goes here... 
    }

    /**
     * Method: getFsTimeName()
     */
    @Test
    public void testGetFsTimeName() throws Exception {
//TODO: Test goes here... 
    }

    /**
     * Method: getFsImageNameCheckpoint()
     */
    @Test
    public void testGetFsImageNameCheckpoint() throws Exception {
//TODO: Test goes here... 
    }

    /**
     * Method: corruptPreUpgradeStorage(File rootDir)
     */
    @Test
    public void testCorruptPreUpgradeStorage() throws Exception {
//TODO: Test goes here... 
    }

    /**
     * Method: getCheckpointDirs(Configuration conf, String defaultName)
     */
    @Test
    public void testGetCheckpointDirs() throws Exception {
//TODO: Test goes here... 
    }

    /**
     * Method: getCheckpointEditsDirs(Configuration conf, String defaultName)
     */
    @Test
    public void testGetCheckpointEditsDirs() throws Exception {
//TODO: Test goes here... 
    }

    /**
     * Method: readString(DataInputStream in)
     */
    @Test
    public void testReadString() throws Exception {
//TODO: Test goes here... 
    }

    /**
     * Method: readString_EmptyAsNull(DataInputStream in)
     */
    @Test
    public void testReadString_EmptyAsNull() throws Exception {
//TODO: Test goes here... 
    }

    /**
     * Method: readBytes(DataInputStream in)
     */
    @Test
    public void testReadBytes() throws Exception {
//TODO: Test goes here... 
    }

    /**
     * Method: writeString(String str, DataOutputStream out)
     */
    @Test
    public void testWriteString() throws Exception {
//TODO: Test goes here... 
    }

    /**
     * Method: getName()
     */
    @Test
    public void testGetName() throws Exception {
//TODO: Test goes here... 
    }

    /**
     * Method: getStorageDirType()
     */
    @Test
    public void testGetStorageDirType() throws Exception {
//TODO: Test goes here... 
    }

    /**
     * Method: isOfType(StorageDirType type)
     */
    @Test
    public void testIsOfTypeType() throws Exception {
//TODO: Test goes here... 
    }

    /**
     * Method: write(DataOutput out)
     */
    @Test
    public void testWriteOut() throws Exception {
//TODO: Test goes here... 
    }

    /**
     * Method: readFields(DataInput in)
     */
    @Test
    public void testReadFields() throws Exception {
//TODO: Test goes here... 
    }

    /**
     * Method: hasNext()
     */
    @Test
    public void testHasNext() throws Exception {
//TODO: Test goes here... 
    }

    /**
     * Method: next()
     */
    @Test
    public void testNext() throws Exception {
//TODO: Test goes here... 
    }

    /**
     * Method: remove()
     */
    @Test
    public void testRemove() throws Exception {
//TODO: Test goes here... 
    }

    /**
     * Method: getRoot()
     */
    @Test
    public void testGetRoot() throws Exception {
//TODO: Test goes here... 
    }

    /**
     * Method: read()
     */
    @Test
    public void testRead() throws Exception {
//TODO: Test goes here... 
    }

    /**
     * Method: read(File from)
     */
    @Test
    public void testReadFrom() throws Exception {
//TODO: Test goes here... 
    }

    /**
     * Method: write()
     */
    @Test
    public void testWrite() throws Exception {
//TODO: Test goes here... 
    }

    /**
     * Method: write(File to)
     */
    @Test
    public void testWriteTo() throws Exception {
//TODO: Test goes here... 
    }

    /**
     * Method: clearDirectory()
     */
    @Test
    public void testClearDirectory() throws Exception {
        fsImage.getStorageDir(0).clearDirectory();
    }

    /**
     * Method: getCurrentDir()
     */
    @Test
    public void testGetCurrentDir() throws Exception {
//TODO: Test goes here... 
    }

    /**
     * Method: getVersionFile()
     */
    @Test
    public void testGetVersionFile() throws Exception {
//TODO: Test goes here... 
    }

    /**
     * Method: getPreviousVersionFile()
     */
    @Test
    public void testGetPreviousVersionFile() throws Exception {
//TODO: Test goes here... 
    }

    /**
     * Method: getPreviousDir()
     */
    @Test
    public void testGetPreviousDir() throws Exception {
//TODO: Test goes here... 
    }

    /**
     * Method: getPreviousTmp()
     */
    @Test
    public void testGetPreviousTmp() throws Exception {
//TODO: Test goes here... 
    }

    /**
     * Method: getRemovedTmp()
     */
    @Test
    public void testGetRemovedTmp() throws Exception {
//TODO: Test goes here... 
    }

    /**
     * Method: getFinalizedTmp()
     */
    @Test
    public void testGetFinalizedTmp() throws Exception {
//TODO: Test goes here... 
    }

    /**
     * Method: getLastCheckpointTmp()
     */
    @Test
    public void testGetLastCheckpointTmp() throws Exception {
//TODO: Test goes here... 
    }

    /**
     * Method: getPreviousCheckpoint()
     */
    @Test
    public void testGetPreviousCheckpoint() throws Exception {
//TODO: Test goes here... 
    }

    /**
     * Method: analyzeStorage(StartupOption startOpt)
     */
    @Test
    public void testAnalyzeStorage() throws Exception {
//TODO: Test goes here... 
    }

    /**
     * Method: doRecover(StorageState curState)
     */
    @Test
    public void testDoRecover() throws Exception {
//TODO: Test goes here... 
    }

    /**
     * Method: lock()
     */
    @Test
    public void testLock() throws Exception {
//TODO: Test goes here... 
    }

    /**
     * Method: tryLock()
     */
    @Test
    public void testTryLock() throws Exception {
//TODO: Test goes here... 
    }

    /**
     * Method: unlock()
     */
    @Test
    public void testUnlock() throws Exception {
//TODO: Test goes here... 
    }


    /**
     * Method: doUpgrade()
     */
    @Test
    public void testDoUpgrade() throws Exception {
//TODO: Test goes here... 
/* 
try { 
   Method method = FSImage.getClass().getMethod("doUpgrade"); 
   method.setAccessible(true); 
   method.invoke(<Object>, <Parameters>); 
} catch(NoSuchMethodException e) { 
} catch(IllegalAccessException e) { 
} catch(InvocationTargetException e) { 
} 
*/
    }

    /**
     * Method: doRollback()
     */
    @Test
    public void testDoRollback() throws Exception {
//TODO: Test goes here... 
/* 
try { 
   Method method = FSImage.getClass().getMethod("doRollback"); 
   method.setAccessible(true); 
   method.invoke(<Object>, <Parameters>); 
} catch(NoSuchMethodException e) { 
} catch(IllegalAccessException e) { 
} catch(InvocationTargetException e) { 
} 
*/
    }

    /**
     * Method: doFinalize(StorageDirectory sd)
     */
    @Test
    public void testDoFinalize() throws Exception {
//TODO: Test goes here... 
/* 
try { 
   Method method = FSImage.getClass().getMethod("doFinalize", StorageDirectory.class); 
   method.setAccessible(true); 
   method.invoke(<Object>, <Parameters>); 
} catch(NoSuchMethodException e) { 
} catch(IllegalAccessException e) { 
} catch(InvocationTargetException e) { 
} 
*/
    }

    /**
     * Method: isParent(String path, String parent)
     */
    @Test
    public void testIsParent() throws Exception {
//TODO: Test goes here... 
/* 
try { 
   Method method = FSImage.getClass().getMethod("isParent", String.class, String.class); 
   method.setAccessible(true); 
   method.invoke(<Object>, <Parameters>); 
} catch(NoSuchMethodException e) { 
} catch(IllegalAccessException e) { 
} catch(InvocationTargetException e) { 
} 
*/
    }

    /**
     * Method: newNamespaceID()
     */
    @Test
    public void testNewNamespaceID() throws Exception {
//TODO: Test goes here... 
/* 
try { 
   Method method = FSImage.getClass().getMethod("newNamespaceID"); 
   method.setAccessible(true); 
   method.invoke(<Object>, <Parameters>); 
} catch(NoSuchMethodException e) { 
} catch(IllegalAccessException e) { 
} catch(InvocationTargetException e) { 
} 
*/
    }

    /**
     * Method: restoreFile(File src, File dstdir, String dstfile)
     */
    @Test
    public void testRestoreFile() throws Exception {
//TODO: Test goes here... 
/* 
try { 
   Method method = FSImage.getClass().getMethod("restoreFile", File.class, File.class, String.class); 
   method.setAccessible(true); 
   method.invoke(<Object>, <Parameters>); 
} catch(NoSuchMethodException e) { 
} catch(IllegalAccessException e) { 
} catch(InvocationTargetException e) { 
} 
*/
    }

    /**
     * Method: saveINode2Image(ByteBuffer name, INode node, DataOutputStream out)
     */
    @Test
    public void testSaveINode2Image() throws Exception {
//TODO: Test goes here... 
/* 
try { 
   Method method = FSImage.getClass().getMethod("saveINode2Image", ByteBuffer.class, INode.class, DataOutputStream.class); 
   method.setAccessible(true); 
   method.invoke(<Object>, <Parameters>); 
} catch(NoSuchMethodException e) { 
} catch(IllegalAccessException e) { 
} catch(InvocationTargetException e) { 
} 
*/
    }

    /**
     * Method: saveImage(ByteBuffer parentPrefix, int prefixLength, INodeDirectory current, DataOutputStream out)
     */
    @Test
    public void testSaveImage() throws Exception {
//TODO: Test goes here... 
/* 
try { 
   Method method = FSImage.getClass().getMethod("saveImage", ByteBuffer.class, int.class, INodeDirectory.class, DataOutputStream.class); 
   method.setAccessible(true); 
   method.invoke(<Object>, <Parameters>); 
} catch(NoSuchMethodException e) { 
} catch(IllegalAccessException e) { 
} catch(InvocationTargetException e) { 
} 
*/
    }

    /**
     * Method: loadFilesUnderConstruction(int version, DataInputStream in, FSNamesystem fs)
     */
    @Test
    public void testLoadFilesUnderConstruction() throws Exception {
//TODO: Test goes here... 
/* 
try { 
   Method method = FSImage.getClass().getMethod("loadFilesUnderConstruction", int.class, DataInputStream.class, FSNamesystem.class); 
   method.setAccessible(true); 
   method.invoke(<Object>, <Parameters>); 
} catch(NoSuchMethodException e) { 
} catch(IllegalAccessException e) { 
} catch(InvocationTargetException e) { 
} 
*/
    }

    /**
     * Method: loadSecretManagerState(int version, DataInputStream in, FSNamesystem fs)
     */
    @Test
    public void testLoadSecretManagerState() throws Exception {
//TODO: Test goes here... 
/* 
try { 
   Method method = FSImage.getClass().getMethod("loadSecretManagerState", int.class, DataInputStream.class, FSNamesystem.class); 
   method.setAccessible(true); 
   method.invoke(<Object>, <Parameters>); 
} catch(NoSuchMethodException e) { 
} catch(IllegalAccessException e) { 
} catch(InvocationTargetException e) { 
} 
*/
    }

    /**
     * Method: getDistributedUpgradeState()
     */
    @Test
    public void testGetDistributedUpgradeState() throws Exception {
//TODO: Test goes here... 
/* 
try { 
   Method method = FSImage.getClass().getMethod("getDistributedUpgradeState"); 
   method.setAccessible(true); 
   method.invoke(<Object>, <Parameters>); 
} catch(NoSuchMethodException e) { 
} catch(IllegalAccessException e) { 
} catch(InvocationTargetException e) { 
} 
*/
    }

    /**
     * Method: getDistributedUpgradeVersion()
     */
    @Test
    public void testGetDistributedUpgradeVersion() throws Exception {
//TODO: Test goes here... 
/* 
try { 
   Method method = FSImage.getClass().getMethod("getDistributedUpgradeVersion"); 
   method.setAccessible(true); 
   method.invoke(<Object>, <Parameters>); 
} catch(NoSuchMethodException e) { 
} catch(IllegalAccessException e) { 
} catch(InvocationTargetException e) { 
} 
*/
    }

    /**
     * Method: setDistributedUpgradeState(boolean uState, int uVersion)
     */
    @Test
    public void testSetDistributedUpgradeState() throws Exception {
//TODO: Test goes here... 
/* 
try { 
   Method method = FSImage.getClass().getMethod("setDistributedUpgradeState", boolean.class, int.class); 
   method.setAccessible(true); 
   method.invoke(<Object>, <Parameters>); 
} catch(NoSuchMethodException e) { 
} catch(IllegalAccessException e) { 
} catch(InvocationTargetException e) { 
} 
*/
    }

    /**
     * Method: verifyDistributedUpgradeProgress(StartupOption startOpt)
     */
    @Test
    public void testVerifyDistributedUpgradeProgress() throws Exception {
//TODO: Test goes here... 
/* 
try { 
   Method method = FSImage.getClass().getMethod("verifyDistributedUpgradeProgress", StartupOption.class); 
   method.setAccessible(true); 
   method.invoke(<Object>, <Parameters>); 
} catch(NoSuchMethodException e) { 
} catch(IllegalAccessException e) { 
} catch(InvocationTargetException e) { 
} 
*/
    }

    /**
     * Method: initializeDistributedUpgrade()
     */
    @Test
    public void testInitializeDistributedUpgrade() throws Exception {
//TODO: Test goes here... 
/* 
try { 
   Method method = FSImage.getClass().getMethod("initializeDistributedUpgrade"); 
   method.setAccessible(true); 
   method.invoke(<Object>, <Parameters>); 
} catch(NoSuchMethodException e) { 
} catch(IllegalAccessException e) { 
} catch(InvocationTargetException e) { 
} 
*/
    }

} 
