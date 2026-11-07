package com.hmdp.service.impl;

import cn.hutool.core.bean.BeanUtil;
import com.hmdp.dto.Result;
import com.hmdp.dto.UserDTO;
import com.hmdp.entity.SeckillVoucher;
import com.hmdp.entity.VoucherOrder;
import com.hmdp.mapper.VoucherOrderMapper;
import com.hmdp.service.ISeckillVoucherService;
import com.hmdp.service.IVoucherOrderService;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.utils.RedisIDWorker;
import com.hmdp.utils.UserHolder;
import lombok.extern.slf4j.Slf4j;
import org.redisson.api.RLock;
import org.redisson.api.RedissonClient;
import org.springframework.aop.framework.AopContext;
import org.springframework.core.io.ClassPathResource;
import org.springframework.data.redis.connection.stream.*;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import javax.annotation.PostConstruct;
import javax.annotation.Resource;
import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;

/**
 * <p>
 *  服务实现类
 * </p>
 *
 * @author 虎哥
 * @since 2021-12-22
 */
@Slf4j
@Service
public class VoucherOrderServiceImpl extends ServiceImpl<VoucherOrderMapper, VoucherOrder> implements IVoucherOrderService {

    @Resource
    private ISeckillVoucherService seckillVoucherService ;

    @Resource
    private RedisIDWorker redisIDWorker ;

    @Resource
    private StringRedisTemplate stringRedisTemplate;

    @Resource
    private RedissonClient redissonClient ;

    // 使用静态代码块 , 加载lua脚本
    private static final DefaultRedisScript<Long> SECKILL_SCRIPT;
    static {
        SECKILL_SCRIPT = new DefaultRedisScript<>() ;
        SECKILL_SCRIPT.setLocation(new ClassPathResource("seckill.lua"));
        SECKILL_SCRIPT.setResultType(Long.class);
    }



    private static final ExecutorService SECKILL_ORDER_EXECUTOR = Executors.newSingleThreadExecutor();
    @PostConstruct
    private void init(){
        SECKILL_ORDER_EXECUTOR.submit(new VoucherOrderHandler()) ;
    }

    private class VoucherOrderHandler implements Runnable{
        String QueueName  = "stream.orders" ;
        @Override
        public void run() {
            while (true) {
                try {
                    // 1. 获取消息队列中的订单信息 XREADGROUP GROUP g1 c1 COUNT 1 BLOCK 2000 STREAMS stream.orders >
                    List<MapRecord<String, Object, Object>> list = stringRedisTemplate.opsForStream().read(
                            Consumer.from("g1", "c1"),
                            StreamReadOptions.empty().count(1).block(Duration.ofSeconds(2)),
                            StreamOffset.create(QueueName, ReadOffset.from(">"))  
                    );
                    // 2. 判断消息获取是否成功
                    if (list == null || list.isEmpty()){
                        // 2.1 如果获取失败, 说明没有消息 , 继续下一次循环
                        continue ;
                    }
                    // 2.2 如果获取成功, 创建订单
                    // 3. 解析消息中的订单信息
                    MapRecord<String, Object, Object> record = list.get(0);
                    Map<Object, Object> value = record.getValue();
                    VoucherOrder voucherOrder = BeanUtil.fillBeanWithMap(value , new VoucherOrder() , true) ;
                    // 3. 创建订单
                    handleVoucherOrder(voucherOrder) ;

                    // 4. 做ACK确认 SACK stream.orders g1 id
                    stringRedisTemplate.opsForStream().acknowledge(QueueName , "g1" , record.getId()) ;
                } catch (Exception e) {
                    log.error("处理订单异常" , e);
                    handlePendingList() ;
                }
            }
        }
    }

    private void handlePendingList() {
        String QueueName  = "stream.orders" ;
            while (true) {
                try {
                    // 1. 获取消息队列中的订单信息 XREADGROUP GROUP g1 c1 COUNT 1  STREAMS stream.orders 0
                    List<MapRecord<String, Object, Object>> list = stringRedisTemplate.opsForStream().read(
                            Consumer.from("g1", "c1"),
                            StreamReadOptions.empty().count(1),
                            StreamOffset.create(QueueName, ReadOffset.from("0"))
                    );
                    // 2. 判断消息获取是否成功
                    if (list == null || list.isEmpty()){
                        // 2.1 如果获取失败, 说明pending-list没有消息 , 结束循环
                        break ;
                    }
                    // 2.2 如果获取成功, 创建订单
                    // 3. 解析消息中的订单信息
                    MapRecord<String, Object, Object> record = list.get(0);
                    Map<Object, Object> value = record.getValue();
                    VoucherOrder voucherOrder = BeanUtil.fillBeanWithMap(value , new VoucherOrder() , true) ;
                    // 3. 创建订单
                    handleVoucherOrder(voucherOrder) ;

                    // 4. 做ACK确认 SACK stream.orders g1 id
                    stringRedisTemplate.opsForStream().acknowledge(QueueName , "g1" , record.getId()) ;
                } catch (Exception e) {
                    log.error("处理pending-list订单异常" , e);
                    try {
                        Thread.sleep(20);
                    } catch (InterruptedException ex) {
                        throw new RuntimeException(ex);
                    }
                }
            }

    }


/*

    private BlockingQueue<VoucherOrder> orderTasks = new ArrayBlockingQueue<>(1024 * 1024) ;
    private class VoucherOrderHandler implements Runnable{
        @Override
        public void run() {
            while (true) {
                try {
                    // 3. 获取阻塞队列中的订单信息
                    VoucherOrder voucherOrder = orderTasks.take();
                    // 4. 创建订单
                    handleVoucherOrder(voucherOrder) ;
                } catch (InterruptedException e) {
                    log.error("处理订单异常");
                }
            }
        }
    }
*/

    private void handleVoucherOrder(VoucherOrder voucherOrder) {
        Long userId = voucherOrder.getUserId();
        RLock lock = redissonClient.getLock("lock:order:" + userId);
        boolean isLock = lock.tryLock();

        // 判断是否获取锁成功
        if (!isLock){
            log.error("不允许重复下单");
            return;
        }
        try {
            proxy.createVoucherOrder(voucherOrder) ;
        } finally {
            lock.unlock();
        }
    }

    private IVoucherOrderService proxy;

    @Override
    public Result seckillVoucher(Long voucherId)  {
        //1. 执行lua脚本, 传入优惠券id和用户id
        Long userId = UserHolder.getUser().getId();
        // 2. 获取订单id
        long orderId = redisIDWorker.nextId("order");
        Long result = stringRedisTemplate.execute(
                SECKILL_SCRIPT,
                Collections.emptyList(),
                voucherId.toString(),
                userId.toString() ,
                String.valueOf(orderId)
        );
        //2. 判断返回结果是否为 0 (0 为 订购成功)
        int r = result.intValue();
        if (r != 0){
            return Result.fail( r == 1 ? "商品库存不足" : "秒杀活动不能重复下单" );
        }
        // 3. 获取代理对象
        proxy = (IVoucherOrderService) AopContext.currentProxy() ;

        //4. 返回订单id
        return Result.ok(0) ;
    }


    // 使用阻塞队列实现 异步秒杀

   /* @Override
    public Result seckillVoucher(Long voucherId)  {
        //1. 执行lua脚本, 传入优惠券id和用户id
        Long userId = UserHolder.getUser().getId();
        Long result = stringRedisTemplate.execute(
                SECKILL_SCRIPT,
                Collections.emptyList(),
                voucherId.toString(),
                userId.toString()
        );
        //2. 判断返回结果是否为 0 (0 为 订购成功)
        int r = result.intValue();
        if (r != 0){
            return Result.fail( r == 1 ? "商品库存不足" : "秒杀活动不能重复下单" );
        }
        //2.1 不为0 , 没有购买资格, 返回异常信息
        //2.2 为0 , 有购买资格
        //3. 将优惠卷id与用户id 存储到阻塞队列
        long orderId = redisIDWorker.nextId("order");
        VoucherOrder voucherOrder = new VoucherOrder();
        //7. 返回订单id
        UserDTO user = UserHolder.getUser();
        //保存订单id
        voucherOrder.setId(orderId);
        //保存用户id
        voucherOrder.setUserId(userId);
        //保存优惠券id
        voucherOrder.setVoucherId(voucherId);

        this.proxy = (IVoucherOrderService) AopContext.currentProxy() ;
        //3.1放入阻塞队列
        orderTasks.add(voucherOrder) ;
        //4. 返回订单id
        return Result.ok(0) ;
    }*/

    @Transactional
    public void createVoucherOrder(VoucherOrder voucherOrder) {
        //5. 一人一单
        //5.1 查询订单
        Long userId = voucherOrder.getUserId();     // 查询用户id

        //5.2 判断是否存在
        Integer count = query().eq("user_id", userId).eq("voucher_id", voucherOrder.getVoucherId()).count();
        //5.3 存在, 返回异常结果
        if (count > 0) {
            log.error("用户 {} 已经参与过秒杀活动", userId);
        }
        //5.4 不存在, 程序继续
        //5. 扣减库存
        //set stock = stock-1 where id = #{id} and stock = #{stock} / -> stock > 0  (乐观锁 CAS法)
        boolean success = seckillVoucherService.update().setSql("stock = stock - 1")
                .eq("voucher_id", voucherOrder.getVoucherId()).gt("stock", 0).update();

        if (!success) {
            // 扣减失败
            log.error("秒杀券库存不足");
        }

        save(voucherOrder);

    }

   /* @Override
    public Result seckillVoucher(Long voucherId) throws InterruptedException {
        //1. 查询
        SeckillVoucher seckillVoucher = seckillVoucherService.getById(voucherId);
        //2. 判断秒杀是否开始
        LocalDateTime beginTime = seckillVoucher.getBeginTime();
        LocalDateTime endTime = seckillVoucher.getEndTime() ;
        //3. 判断秒杀是否结束
        if (beginTime.isAfter(LocalDateTime.now()) || endTime.isBefore(LocalDateTime.now())){
            return Result.fail("未在活动时间内") ;
        }
        //4. 若未结束 , 判断库存
        Integer stock = seckillVoucher.getStock();
        if(stock <=0 ){
            return Result.fail("秒杀券库存不足") ;
        }

        // 先提交事务 , 在释放锁, 避免锁已经释放, 其他查询进来结果事务未提交, 导致订单出现重复问题
        Long userId = UserHolder.getUser().getId();

      *//* // 创建锁对象
        SimpleRedisLock lock = new SimpleRedisLock(stringRedisTemplate, "order:" + userId);
        // 获取锁
        boolean isLock = lock.tryLock(1200);*//*
        RLock lock = redissonClient.getLock("lock:order:" + userId);
        boolean isLock = lock.tryLock(1 , 10 , TimeUnit.SECONDS);

        // 判断是否获取锁成功
        if (!isLock){
            // 获取锁失败 1. 返回错误信息 , 2. 重试
            return  Result.fail("您已经参加过秒杀活动了, 不允许重复下单") ;
        }
        try {
            // 获取代理对象(事务) ,
            IVoucherOrderService proxy = (IVoucherOrderService) AopContext.currentProxy();
            return proxy.createVoucherOrder(voucherId, seckillVoucher);
        } finally {
            lock.unlock();
        }

    }*/
    // 事务注解修饰之后 会产生代理对象 , 但是在上述业务中, 我们调取的是 目标对象 new createVoucherOrder()而非 代理对象
    // 会失去事务功能  , 无法完成 提交 或 回滚
    /*@Transactional
    public Result createVoucherOrder(Long voucherId, SeckillVoucher seckillVoucher) {
        //5. 一人一单
        //5.1 查询订单
        Long userId = UserHolder.getUser().getId();     // 查询用户id

        //5.2 判断是否存在
        Integer count = query().eq("user_id", userId).eq("voucher_id", voucherId).count();
        //5.3 存在, 返回异常结果
        if (count > 0) {
            return Result.fail("您已经参与过秒杀活动了, 无法再次参加");
        }
        //5.4 不存在, 程序继续

        //5. 扣减库存
        //set stock = stock-1 where id = #{id} and stock = #{stock} / -> stock > 0  (乐观锁 CAS法)
        boolean success = seckillVoucherService.update().setSql("stock = stock - 1")
                .eq("voucher_id", voucherId).gt("stock", 0).update();

        if (!success) {
            // 扣减失败
            return Result.fail("秒杀券库存不足");
        }
        //6. 创建订单
        VoucherOrder voucherOrder = new VoucherOrder();
        //7. 返回订单id
        UserDTO user = UserHolder.getUser();
        Long orderId = redisIDWorker.nextId("order");  // 生成订单的全局唯一id
        Long seckillVoucherId = seckillVoucher.getVoucherId();

        voucherOrder.setId(orderId);
        voucherOrder.setUserId(userId);
        voucherOrder.setVoucherId(seckillVoucherId);
        save(voucherOrder);

        return Result.ok(orderId);

    }*/
}
