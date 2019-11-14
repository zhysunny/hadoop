## jvm关闭的钩子
当jvm结束时执行的任务

        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                LOG.info(toStartupShutdownString("SHUTDOWN_MSG: ", new String[]{
                        "Shutting down " + classname + " at " + hostname}));
            }
        });
可测试程序正常停止，异常停止，kill和kill -9  