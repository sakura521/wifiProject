package com.cn.tzspringcloud2.hbase;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.cloud.client.discovery.EnableDiscoveryClient;
import org.springframework.cloud.netflix.eureka.server.EnableEurekaServer;


/**
 * @author: KING
 * @description:
 * @Date:Created in 2019-06-11 11:30
 */
@SpringBootApplication
@EnableEurekaServer
@EnableDiscoveryClient
public class HbaseApplication {

    public static void main(String[] args)
    {
        SpringApplication.run(HbaseApplication.class, args);
    }
}
