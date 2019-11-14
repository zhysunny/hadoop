package org.apache.hadoop.metrics2.impl;

import static org.junit.Assert.*;

import org.junit.*;
import org.apache.hadoop.constant.FilePathConstant;
import org.apache.log4j.xml.DOMConfigurator;

/**
 * MetricsSystemImpl Test
 * @author 章云
 * @date 2019/6/24 22:41
 */
public class MetricsSystemImplTest {

    private MetricsSystemImpl metricsSystem;

    @BeforeClass
    public static void beforeClass() throws Exception {
        System.out.println("Test MetricsSystemImpl Class Start...");
        FilePathConstant.init(FilePathConstant.JUNIT);
        DOMConfigurator.configure(FilePathConstant.LOG4J_TEST_XML);
    }

    @Before
    public void before() throws Exception {
        metricsSystem = new MetricsSystemImpl();
    }

    @After
    public void after() throws Exception {
        metricsSystem = null;
    }

    @AfterClass
    public static void afterClass() throws Exception {
        System.out.println("Test MetricsSystemImpl Class End...");
    }

    /**
     * Method: init(String prefix)
     */
    @Test
    public void testInit() throws Exception {
        metricsSystem.init("NameNode");
    }

    /**
     * Method: start()
     */
    @Test
    public void testStart() throws Exception {
//TODO: Test goes here... 
    }

    /**
     * Method: stop()
     */
    @Test
    public void testStop() throws Exception {
//TODO: Test goes here... 
    }

    /**
     * Method: register(final String name, final String desc, final T source)
     */
    @Test
    public void testRegisterForNameDescSource() throws Exception {
//TODO: Test goes here... 
    }

    /**
     * Method: registerSource(String name, String desc, MetricsSource source)
     */
    @Test
    public void testRegisterSource() throws Exception {
//TODO: Test goes here... 
    }

    /**
     * Method: register(final String name, final String description, final T sink)
     */
    @Test
    public void testRegisterForNameDescriptionSink() throws Exception {
//TODO: Test goes here... 
    }

    /**
     * Method: registerSink(String name, String desc, MetricsSink sink)
     */
    @Test
    public void testRegisterSink() throws Exception {
//TODO: Test goes here... 
    }

    /**
     * Method: register(final Callback callback)
     */
    @Test
    public void testRegisterCallback() throws Exception {
//TODO: Test goes here... 
    }

    /**
     * Method: refreshMBeans()
     */
    @Test
    public void testRefreshMBeans() throws Exception {
//TODO: Test goes here... 
    }

    /**
     * Method: currentConfig()
     */
    @Test
    public void testCurrentConfig() throws Exception {
//TODO: Test goes here... 
    }

    /**
     * Method: onTimerEvent()
     */
    @Test
    public void testOnTimerEvent() throws Exception {
//TODO: Test goes here... 
    }

    /**
     * Method: publishMetricsNow()
     */
    @Test
    public void testPublishMetricsNow() throws Exception {
//TODO: Test goes here... 
    }

    /**
     * Method: publishMetrics(MetricsBuffer buffer, boolean immediate)
     */
    @Test
    public void testPublishMetrics() throws Exception {
//TODO: Test goes here... 
    }

    /**
     * Method: newSink(String name, String desc, MetricsSink sink, MetricsConfig conf)
     */
    @Test
    public void testNewSinkForNameDescSinkConf() throws Exception {
//TODO: Test goes here... 
    }

    /**
     * Method: newSink(String name, String desc, MetricsConfig conf)
     */
    @Test
    public void testNewSinkForNameDescConf() throws Exception {
//TODO: Test goes here... 
    }

    /**
     * Method: getHostname()
     */
    @Test
    public void testGetHostname() throws Exception {
//TODO: Test goes here... 
    }

    /**
     * Method: shutdown()
     */
    @Test
    public void testShutdown() throws Exception {
//TODO: Test goes here... 
    }

    /**
     * Method: preStart()
     */
    @Test
    public void testPreStart() throws Exception {
//TODO: Test goes here... 
    }

    /**
     * Method: postStart()
     */
    @Test
    public void testPostStart() throws Exception {
//TODO: Test goes here... 
    }

    /**
     * Method: preStop()
     */
    @Test
    public void testPreStop() throws Exception {
//TODO: Test goes here... 
    }

    /**
     * Method: postStop()
     */
    @Test
    public void testPostStop() throws Exception {
//TODO: Test goes here... 
    }


    /**
     * Method: startTimer()
     */
    @Test
    public void testStartTimer() throws Exception {
//TODO: Test goes here... 
/* 
try { 
   Method method = MetricsSystemImpl.getClass().getMethod("startTimer"); 
   method.setAccessible(true); 
   method.invoke(<Object>, <Parameters>); 
} catch(NoSuchMethodException e) { 
} catch(IllegalAccessException e) { 
} catch(InvocationTargetException e) { 
} 
*/
    }

    /**
     * Method: snapshotMetrics(MetricsSourceAdapter sa, MetricsBufferBuilder bufferBuilder)
     */
    @Test
    public void testSnapshotMetrics() throws Exception {
//TODO: Test goes here... 
/* 
try { 
   Method method = MetricsSystemImpl.getClass().getMethod("snapshotMetrics", MetricsSourceAdapter.class, MetricsBufferBuilder.class); 
   method.setAccessible(true); 
   method.invoke(<Object>, <Parameters>); 
} catch(NoSuchMethodException e) { 
} catch(IllegalAccessException e) { 
} catch(InvocationTargetException e) { 
} 
*/
    }

    /**
     * Method: stopTimer()
     */
    @Test
    public void testStopTimer() throws Exception {
//TODO: Test goes here... 
/* 
try { 
   Method method = MetricsSystemImpl.getClass().getMethod("stopTimer"); 
   method.setAccessible(true); 
   method.invoke(<Object>, <Parameters>); 
} catch(NoSuchMethodException e) { 
} catch(IllegalAccessException e) { 
} catch(InvocationTargetException e) { 
} 
*/
    }

    /**
     * Method: stopSources()
     */
    @Test
    public void testStopSources() throws Exception {
//TODO: Test goes here... 
/* 
try { 
   Method method = MetricsSystemImpl.getClass().getMethod("stopSources"); 
   method.setAccessible(true); 
   method.invoke(<Object>, <Parameters>); 
} catch(NoSuchMethodException e) { 
} catch(IllegalAccessException e) { 
} catch(InvocationTargetException e) { 
} 
*/
    }

    /**
     * Method: stopSinks()
     */
    @Test
    public void testStopSinks() throws Exception {
//TODO: Test goes here... 
/* 
try { 
   Method method = MetricsSystemImpl.getClass().getMethod("stopSinks"); 
   method.setAccessible(true); 
   method.invoke(<Object>, <Parameters>); 
} catch(NoSuchMethodException e) { 
} catch(IllegalAccessException e) { 
} catch(InvocationTargetException e) { 
} 
*/
    }

    /**
     * Method: configure(String prefix)
     */
    @Test
    public void testConfigure() throws Exception {
//TODO: Test goes here... 
/* 
try { 
   Method method = MetricsSystemImpl.getClass().getMethod("configure", String.class); 
   method.setAccessible(true); 
   method.invoke(<Object>, <Parameters>); 
} catch(NoSuchMethodException e) { 
} catch(IllegalAccessException e) { 
} catch(InvocationTargetException e) { 
} 
*/
    }

    /**
     * Method: configureSystem()
     */
    @Test
    public void testConfigureSystem() throws Exception {
//TODO: Test goes here... 
/* 
try { 
   Method method = MetricsSystemImpl.getClass().getMethod("configureSystem"); 
   method.setAccessible(true); 
   method.invoke(<Object>, <Parameters>); 
} catch(NoSuchMethodException e) { 
} catch(IllegalAccessException e) { 
} catch(InvocationTargetException e) { 
} 
*/
    }

    /**
     * Method: configureSinks()
     */
    @Test
    public void testConfigureSinks() throws Exception {
//TODO: Test goes here... 
/* 
try { 
   Method method = MetricsSystemImpl.getClass().getMethod("configureSinks"); 
   method.setAccessible(true); 
   method.invoke(<Object>, <Parameters>); 
} catch(NoSuchMethodException e) { 
} catch(IllegalAccessException e) { 
} catch(InvocationTargetException e) { 
} 
*/
    }

    /**
     * Method: configureSources()
     */
    @Test
    public void testConfigureSources() throws Exception {
//TODO: Test goes here... 
/* 
try { 
   Method method = MetricsSystemImpl.getClass().getMethod("configureSources"); 
   method.setAccessible(true); 
   method.invoke(<Object>, <Parameters>); 
} catch(NoSuchMethodException e) { 
} catch(IllegalAccessException e) { 
} catch(InvocationTargetException e) { 
} 
*/
    }

    /**
     * Method: clearConfigs()
     */
    @Test
    public void testClearConfigs() throws Exception {
//TODO: Test goes here... 
/* 
try { 
   Method method = MetricsSystemImpl.getClass().getMethod("clearConfigs"); 
   method.setAccessible(true); 
   method.invoke(<Object>, <Parameters>); 
} catch(NoSuchMethodException e) { 
} catch(IllegalAccessException e) { 
} catch(InvocationTargetException e) { 
} 
*/
    }

    /**
     * Method: registerSystemSource()
     */
    @Test
    public void testRegisterSystemSource() throws Exception {
//TODO: Test goes here... 
/* 
try { 
   Method method = MetricsSystemImpl.getClass().getMethod("registerSystemSource"); 
   method.setAccessible(true); 
   method.invoke(<Object>, <Parameters>); 
} catch(NoSuchMethodException e) { 
} catch(IllegalAccessException e) { 
} catch(InvocationTargetException e) { 
} 
*/
    }

    /**
     * Method: initSystemMBean()
     */
    @Test
    public void testInitSystemMBean() throws Exception {
//TODO: Test goes here... 
/* 
try { 
   Method method = MetricsSystemImpl.getClass().getMethod("initSystemMBean"); 
   method.setAccessible(true); 
   method.invoke(<Object>, <Parameters>); 
} catch(NoSuchMethodException e) { 
} catch(IllegalAccessException e) { 
} catch(InvocationTargetException e) { 
} 
*/
    }

} 
