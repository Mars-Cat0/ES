package com.zhiyou.test;

import com.alibaba.fastjson.JSONObject;
import com.zhiyou.model.User;
import org.apache.http.HttpHost;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;

import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.junit.jupiter.api.Test;

import javax.print.Doc;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public class TestES {



    /*
     创建索引
     */
    @Test
    public void TestJavsEs() throws IOException {

        // 1. 创建客户端
        RestHighLevelClient client = new RestHighLevelClient(
                RestClient.builder(
                        new HttpHost("192.168.190.128",9200,"http")
                )
        );

        // 2. 创建索引
        CreateIndexRequest request = new CreateIndexRequest("user2");

        // 2-1,索引setting配置
        request.settings(Settings.builder().
        put("index.number_of_shards",5) // 分片
                .put("index.number_of_replicas",1)
        );

        // 3. 构建mappings - 3中方式 - 这里使用json格式
        String jsonValue = "{\n" +
                "  \"user2\": {\n" +
                "    \"properties\": {\n" +
                "      \"name\": {\n" +
                "        \"type\": \"text\",\n" +
                "\t\t\"analyzer\":\"ik_max_word\"\n" +
                "      },\n" +
                "      \"password\": {\n" +
                "        \"type\": \"text\"\n" +
                "      }\n" +
                "    }\n" +
                "  }\n" +
                "}      ";
        request.mapping("user2",jsonValue, XContentType.JSON);

        // 4. 发送请求
        client.indices().create(request);

        // 5. 关闭客户端
        client.close();

        System.out.println("success");

    }

    /*
    添加数据
     */
    @Test
    public void testAddES() throws IOException {
        // 0 创建客户端
        RestHighLevelClient client = new RestHighLevelClient(
               RestClient.builder(
                      new HttpHost("192.168.190.128",9200,"http")
               )
        );

        // 1 创建插入的请求
        IndexRequest request = new IndexRequest("book2","novel","1");

        // 2 构造json字符串,用于插入数据
        String jsonStr = "{\"title\":\"aaa\",\"content\":\"aaa\"}";

        // 3. 把上一步的json字符串设置进请求中
        request.source(jsonStr,XContentType.JSON);

        // 4. 发送请求
        IndexResponse response = client.index(request);

        // 5. 判断有木有插入成功
        if(response.getResult() == DocWriteResponse.Result.CREATED){
            System.out.println("success");
        }else {
            System.out.println("fail");
        }

        // 6 关
        client.close();

        System.out.println("over");
    }

    /*
    批量插入
    1 从数据库查出全部
    2 遍历List
        将其中的对象转成json字符串 -> FastJson
        将转成json放入Lsit中
    3 遍历该存储Json的list
        遍历过程中将Json取出
     */

    @Test
    public void  testAddsES() throws IOException {

        List<User> users = new ArrayList<>();
        users.add(new User("中华人民共和国","123"));
        users.add(new User("中华","123"));
        users.add(new User("中华牙膏","123"));
        users.add(new User("华人","123"));
        users.add(new User("民国","123"));
        users.add(new User("zhonghua","123"));

        List<String> userJson = new ArrayList<>();
        for (User user: users) {
            userJson.add(JSONObject.toJSONString(user));
        }

        // 0 创建客户端
        RestHighLevelClient client = new RestHighLevelClient(
                RestClient.builder(
                        new HttpHost("192.168.190.128",9200,"http")
                )
        );

        // 1 创建批量请求
        BulkRequest request = new BulkRequest();

        // 2 创建创建请求[索引,类型,id] 写json字符串 将json设置进请求

        for (int i = 0;i < userJson.size();i++){

            request.add(new IndexRequest("user2","user2").
                    source(userJson.get(i),XContentType.JSON));
        }

        // 3 发出请求

        BulkResponse response = client.bulk(request);
        for (BulkItemResponse bulkItemResponse: response) {
            if(bulkItemResponse.getOpType() == DocWriteRequest.OpType.INDEX
                || bulkItemResponse.getOpType() == DocWriteRequest.OpType.CREATE){
                System.out.println("success");
            }

        }

        client.close();

        System.out.println("over");
    }

    /*
    查询
     */
    @Test
    public void testSearchES() throws IOException {
        // 0 创建客户端
        RestHighLevelClient client = new RestHighLevelClient(
                RestClient.builder(
                        new HttpHost("192.168.190.128",9200,"http")
                )
        );

        // 1 获得[查询]请求
        SearchRequest searchRequest = new SearchRequest("user2");
        searchRequest.types("user2");

        // 2 使用SearchSourceBuilder来构造查询请求体
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        BoolQueryBuilder boolQueryBuilder = QueryBuilders.boolQuery();
        boolQueryBuilder.must(QueryBuilders.matchQuery("name","中华人民"));
        sourceBuilder.query(boolQueryBuilder);
        sourceBuilder.from(0);
        sourceBuilder.size(10);

        // 3将请求体加入请求中
        searchRequest.source(sourceBuilder);

        // 4 发送请求
        SearchResponse searchResponse = client.search(searchRequest);

        // 5 处理搜索命中文档结果
        SearchHits hits = searchResponse.getHits();
        System.out.println("共"+hits.getTotalHits());
        SearchHit[] searchHits = hits.getHits();
        for (SearchHit hit: searchHits) {
            String index = hit.getIndex();
            String type = hit.getType();
            String id = hit.getId();
            float score = hit.getScore();
            Map<String, Object> sourceAsMap = hit.getSourceAsMap();
            System.out.println(sourceAsMap.toString());

        }

        // 5 关闭客户端
        client.close();
    }
}
