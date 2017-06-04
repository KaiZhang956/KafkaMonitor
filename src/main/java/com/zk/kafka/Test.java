package com.zk.kafka;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.tools.JavaCompiler;
import javax.tools.ToolProvider;

/**
 * Created by ZK on 2017/5/4.
 */
public class Test {
    private static final Logger log = LoggerFactory.getLogger(Test.class);
    public static void main(String[] args) throws InterruptedException {
        Runnable x = new MonitorThread("highrequest","192.168.0.10","test","test","192.168.0.10",2,6000);
        Thread thread = new Thread(x);
        thread.start();
    }
}
