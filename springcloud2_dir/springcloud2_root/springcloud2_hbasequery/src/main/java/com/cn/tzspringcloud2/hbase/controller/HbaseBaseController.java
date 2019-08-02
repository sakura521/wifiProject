package com.cn.tzspringcloud2.hbase.controller;


import com.cn.tzspringcloud2.hbase.service.HbaseBaseService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.*;

import javax.annotation.Resource;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * @author: KING
 * @description:
 * @Date:Created in 2019-06-11 12:07
 */
@Controller
@RequestMapping(value = "/hbase")
public class HbaseBaseController {
    private static Logger LOG = LoggerFactory.getLogger(HbaseBaseController.class);

    @Resource
    private HbaseBaseService hbaseBaseService;

    @ResponseBody
    @RequestMapping(value="/test",method={RequestMethod.GET,RequestMethod.POST})
    public String test(@RequestParam(name = "field") String field,
                        @RequestParam(name = "fieldValue") String fieldValue){
        System.out.println(field);
        System.out.println(fieldValue);
        return field  + fieldValue;
    }

    @ResponseBody
    @RequestMapping(value="/search/{table}/{rowkey}", method={RequestMethod.GET,RequestMethod.POST})
    public Set<String> search(@PathVariable(value = "table") String table,
                                 @PathVariable(value = "rowkey") String rowkey){

        return hbaseBaseService.getSingleColumn(table,rowkey);
    }

    /**
     * 通过 tableName,rowkey获取  mac多版本数据    根据mac获取轨迹信息
     * HBase做查询的特点？？？es做查询区别
     * @param table
     * @param rowkey
     * @return
     */
    @ResponseBody
    @RequestMapping(value="/search1", method={RequestMethod.GET,RequestMethod.POST})
    public Set<String> search1( @RequestParam(name = "table") String table,
                                @RequestParam(name = "rowkey") String rowkey){
        //通过二级索引去找主关联表的rowkey 这个rowkey就是MAC 多版本
//        HbaseBaseService hbaseBaseService=new HbaseBaseService();
        return hbaseBaseService.getSingleColumn(table,rowkey,20);
    }

    @ResponseBody
    @RequestMapping(value="/getRelation", method={RequestMethod.GET,RequestMethod.POST})
    public Map<String,List<String>> getRelation(@RequestParam(name = "field") String field,
                                                @RequestParam(name = "fieldValue") String fieldValue){
//        HbaseBaseService hbaseBaseService=new HbaseBaseService();
        return hbaseBaseService.getRealtion(field,fieldValue);
    }

    public static void main(String[] args) {
        HbaseBaseController hbaseBaseController = new HbaseBaseController();
        hbaseBaseController.getRelation("send_mail", "1323243@qq.com");
    }
}
