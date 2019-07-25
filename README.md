# wifi
基于flume+kafka+HBase+spark+ElasticSearch的用户轨迹查询大数据开发项目

项目名称：实时的用户轨迹查询项目

项目介绍：
    利用企业建设的WIFI基站，实时采集用户的信息，可以基于这些信息做用户画像处理，网络安全监控，精准营销等；
	
    
项目架构：
    主要是基于Flume+Kafka+Sparkstreaming +HBase+ES来实现实时的用户信息存储轨迹查询任务。

项目组成：

第一部分：数据采集，Flume采集日志文件传输到kafka的topic里面；

第二部分：kafka作为数据总线，消息中间件，数据缓冲层；

第三部分：数据处理部分，spark streaming 消费kafka数据进行实时的HBase存储和关联存储；全量消息存入ES

第四部分：数据接口微服务层：提供了HBase的数据查询接口，关联查询；ES实现用户的轨迹信息查询。


项目数据结构：
(1) 日志文件命名
数据类型_来源_UUID.txt ：
  如BASE_SOURCE_UUID.txt
    wechat_source1_1111160.txt
    qq_source1_21111221.txt
    mail_source2222_32132137.txt
(2) 通用字段
