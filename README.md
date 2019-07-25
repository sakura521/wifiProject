# wifi
基于flume+kafka+HBase+spark+ElasticSearch的用户轨迹查询大数据开发项目

项目名称：实时的用户轨迹查询项目
项目介绍：
    利用企业建设的WIFI基站，实时采集用户的信息，可以基于这些信息做用户画像处理，网络安全监控，精准营销等；
项目架构：
    主要是基于Flume+Kafka+Sparkstreaming +HBase+ES来实现实时的用户信息存储轨迹查询任务。


第一部分：数据采集，Flume采集日志文件传输到kafka的topic里面；
第二部分：kafka作为数据总线，消息中间件，数据缓冲层；
第三部分：数据处理部分，spark streaming 消费kafka数据进行实时的HBase存储和关联存储；全量消息存入ES
第四部分：数据接口层：提供了HBase的数据查询接口，关联查询；ES实现用户的轨迹信息查询。

项目数据结构：
(1) 日志文件命名
数据类型_来源_UUID.txt ：
  如BASE_SOURCE_UUID.txt
    wechat_source1_1111160.txt
    qq_source1_21111221.txt
    mail_source2222_32132137.txt
(2) 通用字段


数据举例：
mail = imei,imsi,longitude,latitude,phone_mac,device_mac,device_number,collect_time,send_mail,send_time,accept_mail,accept_time,mail_content,mail_type 
6923504955  6689755621  77.135637   83.077520   Le-eJ-QU-ZT-lh-aP   Lz-Db-bv-MD-Hp-Zt   30375814    1529848025  7kt86rmn0e4ux1p7@263.net    1520590719  xx88yc8ap2u5wu4dxfnl@msn.com    1533468318 accept
​
qq = imei,imsi,longitude,latitude,phone_mac,device_mac,device_number,collect_time,username,phone,object_username,send_message,accept_message,message_time
6503513360  4461958276  76.284357   22.878862   fT-Qo-CE-zJ-hj-uD   rZ-eR-tc-Xz-EE-lu   00744006    1539925535  柳祥  15107290985 戚龙林 15606128228
wechat = imei,imsi,longitude,latitude,phone_mac,device_mac,device_number,collect_time,username,phone,object_username,send_message,accept_message,message_time
6192560851  5304747702  81.366260   36.713947   ZG-aY-Mm-Oj-My-jt   nQ-cJ-Sy-Iy-ou-Wq   17679549    1538799439  慎军亨 13103413047 隗致  13700350350
