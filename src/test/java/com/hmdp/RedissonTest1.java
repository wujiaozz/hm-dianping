package com.hmdp;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.redisson.api.RLock;
import org.redisson.api.RedissonClient;
import org.springframework.boot.test.context.SpringBootTest;

import javax.annotation.Resource;
import java.util.concurrent.TimeUnit;

@SpringBootTest
public class RedissonTest1 {
   /* @Resource
    private RedissonClient redissonClient ;

    @Resource(name = "redissonClient2")
    private RedissonClient redissonClient2 ;

    @Resource(name = "redissonClient3")
    private RedissonClient redissonClient3 ;

    private  RLock lock ;

    @BeforeEach
    void setUp(){
        RLock lock1 = redissonClient.getLock("order") ;
        RLock lock2 = redissonClient2.getLock("order") ;
        RLock lock3 = redissonClient3.getLock("order") ;

        // 创建联锁 multiLock
        lock = redissonClient.getMultiLock(lock1, lock2, lock3);
    }
*/
  /*  @Test
    void testRedisson() throws InterruptedException {
        // 获取锁(可重入) , 指定锁的名称
        //RLock lock = redissonClient.getLock("anyLock") ;
        // 尝试获取锁, 参数分别是 : 获取锁的最大等待时间(期间会重试) , 锁自动释放时间 , 单位时间
        boolean isLock = lock.tryLock(80 , 100 , TimeUnit.SECONDS) ;
        if (isLock){
            try{
                System.out.println("执行业务");
            }finally {
                // 释放锁
                lock.unlock();
            }
        }
    }*/




}
