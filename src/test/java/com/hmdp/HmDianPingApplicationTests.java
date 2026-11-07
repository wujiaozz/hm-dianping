package com.hmdp;

import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.entity.Shop;
import com.hmdp.service.impl.ShopServiceImpl;
import com.hmdp.utils.CacheClient;
import com.hmdp.utils.RedisIDWorker;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.data.geo.Point;
import org.springframework.data.redis.connection.RedisGeoCommands;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;

import java.util.*;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static com.hmdp.utils.RedisConstants.CACHE_SHOP_KEY;
import static com.hmdp.utils.RedisConstants.CACHE_SHOP_TTL;

@SpringBootTest
class HmDianPingApplicationTests {

    @Resource
    private ShopServiceImpl shopService ;

    @Resource
    private CacheClient cacheClient ;

    @Resource
    private RedisIDWorker redisIDWorker ;

    @Resource
    private StringRedisTemplate stringRedisTemplate ;

    private ExecutorService es  = Executors.newFixedThreadPool(500) ;

    @Test
    void loadShopData(){
        //1. 查询店铺信息
        List<Shop> list = shopService.list();
        //2. 把店铺分组 按照type_id , id一致的放到一个集合
        Map<Long , List<Shop>> map = list.stream().collect(Collectors.groupingBy(Shop::getTypeId)) ;
        //3. 分批存储写入
        for (Map.Entry<Long, List<Shop>> entry : map.entrySet()) {
            //3.1 获取类型id
            Long typeId = entry.getKey();
            String key = "shop:geo:" + typeId ;
            //3.2 获取同类型的店铺的集合
            List<Shop> value = entry.getValue();
            List<RedisGeoCommands.GeoLocation<String>> locations = new ArrayList<>(value.size()) ;
            //3.3 写入reids  geoadd key 经度 纬度 member
            for (Shop shop : value) {
                Double x = shop.getX();
                Double y = shop.getY();
                //stringRedisTemplate.opsForGeo().add(key , new Point(x,y), shop.getId().toString()) ;
                locations.add(new RedisGeoCommands.GeoLocation<>(shop.getId().toString() , new Point(x,y))) ;
            }
            stringRedisTemplate.opsForGeo().add(key , locations) ;
        }
    }


    @Test
    void testSaveShop() throws InterruptedException {
        Shop shop = shopService.getById(1L);

        cacheClient.setWithLogicalExpire(CACHE_SHOP_KEY + 1L, shop , 10L , TimeUnit.SECONDS);
    }

    @Test
    void testIDWorker() {
        Runnable task = () -> {
            for (int i = 0; i < 100; i++) {
                long id = redisIDWorker.nextId("order");
                System.out.println(id);
            }
        } ;
        for (int i = 0; i < 300; i++) {
            es.submit(task) ;
        }
    }
}
