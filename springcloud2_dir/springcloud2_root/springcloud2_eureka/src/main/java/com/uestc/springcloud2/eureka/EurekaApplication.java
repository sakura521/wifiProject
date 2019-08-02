package com.uestc.springcloud2.eureka;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.cloud.netflix.eureka.server.EnableEurekaServer;

/**
 * 注册中心
 */
@SpringBootApplication
@EnableEurekaServer
public class EurekaApplication
{
    public static void main( String[] args )
    {
        SpringApplication.run(EurekaApplication.class, args);
    }
}
