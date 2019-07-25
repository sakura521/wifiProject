package com.cn.tzspringcloud2.es.service;



import com.uestc.bigdata.es.jestService.JestServse;
import com.uestc.bigdata.es.jestService.ResultParse;
import io.searchbox.client.JestClient;
import io.searchbox.core.SearchResult;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Map;



/**
 * @author: KING
 * @description:
 * @Date:Created in 2019-06-13 20:18
 */
@Service
public class EsBaseService {

    // 传时间范围   比如你要查3天之内的轨迹  轨迹查询  根据mac查询 是所有数据都有的字段
    public List<Map<String, Object>> getLocus(String mac){
        //实现查询
        JestClient jestClient = null;
        List<Map<String, Object>> maps = null;
        String[] includes = new String[]{"latitude","longitude","collect_time"};
        try {
            jestClient = JestServse.getJestClient();
            // term查询要加入keyword 当做一个整体不拆分
            SearchResult search = JestServse.search(jestClient,
                    "",
                    "",
                    "phone_mac.keyword",
                    mac,
                    "collect_time",
                    "asc",
                    1,
                    2000,
                    includes);
            maps = ResultParse.parseSearchResultOnly(search);
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            JestServse.closeJestClient(jestClient);
        }
        return maps;
    }

    public static void main(String[] args) {
        EsBaseService esBaseService = new EsBaseService();
        List<Map<String, Object>> locus = esBaseService.getLocus("aa-aa-aa-aa-aa-aa");
        System.out.println(locus.size());
    }
}
