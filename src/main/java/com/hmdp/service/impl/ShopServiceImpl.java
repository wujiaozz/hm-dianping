package com.hmdp.service.impl;

import cn.hutool.core.util.BooleanUtil;
import cn.hutool.core.util.StrUtil;
import cn.hutool.json.JSONObject;
import cn.hutool.json.JSONUtil;
import com.baomidou.mybatisplus.extension.plugins.pagination.Page;
import com.hmdp.dto.Result;
import com.hmdp.entity.Shop;
import com.hmdp.mapper.ShopMapper;
import com.hmdp.service.IShopService;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.utils.CacheClient;
import com.hmdp.utils.RedisConstants;
import com.hmdp.utils.RedisData;
import com.hmdp.utils.SystemConstants;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.geo.Distance;
import org.springframework.data.geo.GeoResult;
import org.springframework.data.geo.GeoResults;
import org.springframework.data.geo.Point;
import org.springframework.data.redis.connection.RedisGeoCommands;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.domain.geo.GeoReference;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.util.StringUtils;
import org.springframework.web.multipart.support.StandardServletMultipartResolver;

import javax.annotation.Resource;

import java.time.LocalDateTime;
import java.util.*;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static com.hmdp.utils.RedisConstants.*;

/**
 * <p>
 *  服务实现类
 * </p>
 *
 * @author 虎哥
 * @since 2021-12-22
 */
@Service
public class ShopServiceImpl extends ServiceImpl<ShopMapper, Shop> implements IShopService {

    @Resource
    private StringRedisTemplate stringRedisTemplate ;
    @Autowired
    private StandardServletMultipartResolver standardServletMultipartResolver;

    @Resource
    private CacheClient cacheClient ;


    @Override
    public Result queryById(Long id) {
        // 逻辑过期解决缓存击穿
        Shop shop = cacheClient.queryWithLogicalExpire(CACHE_SHOP_KEY , id , Shop.class ,
                this::getById , CACHE_SHOP_TTL , TimeUnit.MINUTES) ;


        if (shop == null){
            return Result.fail("店铺不存在") ;
        }
        //6.  返回信息

        return Result.ok(shop);
    }


    @Override
    @Transactional
    public Result update(Shop shop) {
        Long id = shop.getId();
        if (id == null){
            return Result.fail("店铺id不能为空") ;
        }
        // 1. 更新数据库
        updateById(shop) ;
        // 2. 删除缓存
        stringRedisTemplate.delete(CACHE_SHOP_KEY + id) ;
        //3. 返回结果
        return Result.ok() ;
    }

    @Override
    public Result queryShopByType(Integer typeId, Integer current, Double x, Double y) {
        //1.判断是否需要根据坐标查询
        if(x == null || y == null){
            // 不需要坐标查询 ,按数据库查
            // 根据类型分页查询
            Page<Shop> page = query()
                    .eq("type_id" , typeId)
                    .page(new Page<>(current , SystemConstants.DEFAULT_PAGE_SIZE)) ;
            //返回数据
            return Result.ok(page) ;
        }
        //2.计算分页参数
        int from = (current - 1) * SystemConstants.DEFAULT_PAGE_SIZE ;
        int end = current *SystemConstants.DEFAULT_PAGE_SIZE ;
        //3.查询redis , 按照距离排序 , 分页 , 结果 : shopId , distance
        String key = "shop:geo:" + typeId ;
        GeoResults<RedisGeoCommands.GeoLocation<String>> results = stringRedisTemplate.opsForGeo().
                search(
                        key
                        , GeoReference.fromCoordinate(x, y)
                        , new Distance(5000)
                        , RedisGeoCommands.GeoSearchCommandArgs.newGeoSearchArgs().includeDistance().limit(end)
                );
        //4.解析出id
        if (results == null){
            return Result.ok() ;
        }
        List<GeoResult<RedisGeoCommands.GeoLocation<String>>> list = results.getContent();
        List<Long> ids = new ArrayList<>(list.size()) ;
        Map<String , Distance> distanceMap = new HashMap<>(list.size()) ;
        //4.1 截取从from到end的数据
        list.stream().skip(from).forEach(result -> {
            // 4.2 获取店铺id
            String shopId = result.getContent().getName();
            ids.add(Long.valueOf(shopId)) ;
            //4.3 获取距离
            Distance distance = result.getDistance();
            distanceMap.put(shopId , distance) ;
        });
        if (ids.isEmpty()){
            return Result.ok(Collections.emptyList()) ;
        }
        //5.根据id查询店铺
        String idStr = StrUtil.join("," , ids);
        List<Shop> shops = query().in("id", ids).last("ORDER BY FIELD ( id ," + idStr + ")").list();
        for (Shop shop : shops) {
            Distance distance = distanceMap.get(shop.getId().toString());
            shop.setDistance(distance.getValue()) ;

        }
        //6. 返回
        return Result.ok(shops);
    }
}
