package com.cn.tzspringcloud2.es.controller;


import com.cn.tzspringcloud2.es.feign.HbaseFeign;
import com.cn.tzspringcloud2.es.service.EsBaseService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;

import javax.annotation.Resource;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * @author: KING
 * @description:
 * @Date:Created in 2019-06-13 20:18
 */
@Controller
@RequestMapping(value = "/es")
public class EsBaseController {

    @Resource
    private EsBaseService esBaseService;


    @Autowired
    private HbaseFeign hbaseFeign;


/**
     * 根据任意条件查找轨迹数据(时间、空间)
     * @param field
     * @param fieldValue
     * @return
     */
    @ResponseBody
    @RequestMapping(value = "/getLocus", method = {RequestMethod.GET, RequestMethod.POST})
    public List<Map<String, Object>> getLocus(@RequestParam(name = "field") String field,
                                                 @RequestParam(name = "fieldValue") String fieldValue) {

        System.out.println("==========");
        Set<String> macs = hbaseFeign.search1(field, fieldValue);
        System.out.println("=================");
        // 根据数据类型, 排序，分页
        // indexName typeName
        // sortField sortValue
        // pageNumber  pageSize
        // 查找一个mac
        String mac = macs.iterator().next();
        return  esBaseService.getLocus(mac);
    }

    public static void main(String[] args) {
        EsBaseController esBaseController = new EsBaseController();
        List<Map<String, Object>> locus = esBaseController.getLocus("test:username", "andiy");
        System.out.println(locus.size());
    }
}
