package com.cn.tzspringcloud2.es;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.cloud.client.discovery.EnableDiscoveryClient;
import org.springframework.cloud.netflix.eureka.server.EnableEurekaServer;
import org.springframework.cloud.netflix.feign.EnableFeignClients;


/**
 * @author: KING
 * @description:
 * @Date:Created in 2019-06-13 20:10
 */
@SpringBootApplication
@EnableEurekaServer
@EnableDiscoveryClient
@EnableFeignClients
public class ESqueryApplication {
    public static void main(String[] args) {

        SpringApplication.run(ESqueryApplication.class,args);
    }
}
