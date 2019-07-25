package com.uestc.bigdata.es.jestService;

import io.searchbox.client.JestClient;
import io.searchbox.core.SearchResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * @author:
 * @description: 解析器
 * @Date:Created in 2018-05-13 07:47
 */
public class ResultParse {
    private static Logger LOG = LoggerFactory.getLogger(ResultParse.class);



    public static void main(String[] args) throws Exception {
        JestClient jestClient = JestServse.getJestClient();

    }

    /**
     * 解析listMap
     * 结果格式为  {hits=0, total=0, data=[]}
     * @param search
     * @return
     */
    public static List<Map<String,Object>> parseSearchResultOnly(SearchResult search){

        List<Map<String,Object>> list = new ArrayList<Map<String,Object>>();
        List<SearchResult.Hit<Object, Void>> hits = search.getHits(Object.class);
        for(SearchResult.Hit<Object, Void> hit : hits){
            Map<String,Object> source = (Map<String,Object>)hit.source;
            list.add(source);
        }
        return list;
    }
}
