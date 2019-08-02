package com.cn.tzspringcloud2.es.feign;


import org.springframework.cloud.netflix.feign.FeignClient;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;

import java.util.Set;

@FeignClient("sakura-springcloud-hbasequery")
public interface HbaseFeign {

    @ResponseBody
    @RequestMapping(value="/hbase/search1", method={RequestMethod.GET})
    public Set<String> search1(@RequestParam(name = "table") String table,
                               @RequestParam(name = "rowkey") String rowkey);
}

