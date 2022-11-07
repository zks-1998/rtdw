package utils;

import org.omg.CORBA.TIMEOUT;

import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class ThreadPoolUtil {
    // 私有化构造器
    private ThreadPoolUtil(){
    }
    private volatile static ThreadPoolExecutor threadPoolExecutor;

    public static ThreadPoolExecutor getPool(){
        if(threadPoolExecutor == null){
            synchronized (ThreadPoolUtil.class){
                if(threadPoolExecutor == null){
                    threadPoolExecutor = new ThreadPoolExecutor(
                            4,
                            60,
                            300,
                            TimeUnit.SECONDS,
                            new LinkedBlockingDeque<Runnable>(Integer.MAX_VALUE));
                }
            }
        }
        return threadPoolExecutor;
    }
}
