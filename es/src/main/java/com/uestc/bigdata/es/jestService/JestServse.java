package com.uestc.bigdata.es.jestService;

import com.google.gson.GsonBuilder;
import org.apache.commons.lang.StringUtils;
import io.searchbox.client.JestClient;
import io.searchbox.client.JestClientFactory;
import io.searchbox.client.config.HttpClientConfig;
import io.searchbox.core.Search;
import io.searchbox.core.SearchResult;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.sort.SortOrder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.Map;

public class JestServse {
    private static Logger LOG = LoggerFactory.getLogger(JestServse.class);

    public static JestClient getJestClient(){
        JestClientFactory factory = new JestClientFactory();
        factory.setHttpClientConfig(new HttpClientConfig
                .Builder("http://linux4:9200")
                .multiThreaded(true)
                .gson(new GsonBuilder().setDateFormat("yyyy-MM-dd'T'hh:mm:ss").create())
                .connTimeout(1500)
                .readTimeout(3000)
                .build());
        return factory.getObject();
    }

    /**
     * 关闭连接
     * @param jestServse
     */
    public static void   closeJestClient(JestClient jestServse){
        if(jestServse!=null){
            jestServse.shutdownClient();
        }
    }


    public static SearchResult search(JestClient jestClient,
                                      String indexName,
                                      String typeName,
                                      String field,
                                      String fieldValue,
                                      String sortField,
                                      String sortValue,
                                      int pageNumber,
                                      int pageSize,
                                      String[] includes){
        // 构造查询体
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();

        searchSourceBuilder.fetchSource(includes,new String[0]);

        // 构造查询语句
        BoolQueryBuilder boolQueryBuilder = QueryBuilders.boolQuery();
        if(StringUtils.isEmpty(field)){
            boolQueryBuilder= boolQueryBuilder.must(QueryBuilders.matchAllQuery());
        }else{
            boolQueryBuilder= boolQueryBuilder.must(QueryBuilders.termQuery(field,fieldValue));
        }
        searchSourceBuilder.query(boolQueryBuilder);

        // 定义分页

        searchSourceBuilder.from((pageNumber-1)*pageSize);
        searchSourceBuilder.size(pageSize);

        // 排序

        if("DESC".equals(sortValue)){
            searchSourceBuilder.sort(sortField, SortOrder.DESC);
        }else{
            searchSourceBuilder.sort(sortField, SortOrder.ASC);
        }
        System.out.println("sql =====" + searchSourceBuilder.toString());

        // 构造查询执行器
        Search.Builder builder = new Search.Builder(searchSourceBuilder.toString());

        // 设置 index type
        if(StringUtils.isNotBlank(indexName)){
            builder.addIndex(indexName);
        }

        if(StringUtils.isNotBlank(typeName)){
            builder.addType(typeName);
        }

        // 执行查询结果
        SearchResult searchResult=null;
        Search build = builder.build();
        try {
            searchResult = jestClient.execute(build);
        } catch (IOException e) {
            LOG.error("查询失败",e);
        }

        return searchResult;
    }

    public static void main(String[] args) {
        String[] includes={"latitude","longitude"};
        JestClient jestClient = JestServse.getJestClient();
        SearchResult search = search(jestClient,
                "",
                "",
                "phone_mac.keyword",
                "kt-Wh-tV-oP-aJ-rc",
                "collect_time",
                "desc",
                1,
                20,
                includes);
        List<Map<String, Object>> maps = ResultParse.parseSearchResultOnly(search);
        System.out.println(maps.size());
    }
}
