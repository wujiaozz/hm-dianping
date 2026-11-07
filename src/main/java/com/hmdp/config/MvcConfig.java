package com.hmdp.config;

import com.hmdp.utils.LoginInterceptor;
import com.hmdp.utils.RefreshTokenInterceptor;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.web.servlet.config.annotation.InterceptorRegistry;
import org.springframework.web.servlet.config.annotation.WebMvcConfigurer;

import javax.annotation.Resource;

@Configuration
public class MvcConfig implements WebMvcConfigurer {

    @Resource
    private StringRedisTemplate stringRedisTemplate ;


    @Override
    public void addInterceptors(InterceptorRegistry registry) {
        // 需要登录才能访问的路径拦截器
        registry.addInterceptor(new LoginInterceptor()).excludePathPatterns(
                "/user/code" , "/user/login" , "/blog/hot" , "/shop/**" , "/shop-type/**"
                 , "/voucher/**" , "/upload/**"
        ).order(1) ;
        //  对token刷新刷新的拦截器, 拦截一切路径
        registry.addInterceptor(new RefreshTokenInterceptor(stringRedisTemplate)).addPathPatterns("/**").order(0) ;
    }
}
