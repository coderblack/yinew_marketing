## 创建事件明细表
```
allow_experimental_map_type = 1;
create table default.yinew_detail
(
    account           String   ,
    appId             String   ,
    appVersion        String   ,
    carrier           String   ,
    deviceId          String   ,
    deviceType        String   ,
    eventId           String   ,
    ip                String   ,
    latitude          Float64  ,
    longitude         Float64  ,
    netType           String   ,
    osName            String   ,
    osVersion         String   ,
    properties        Map(String,String),
    releaseChannel    String,
    resolution        String,
    sessionId         String,
    timeStamp         Int64 ,
    INDEX u (deviceId) TYPE minmax GRANULARITY 3,
    INDEX t (timeStamp) TYPE minmax GRANULARITY 3
) ENGINE = MergeTree()
ORDER BY (deviceId,timeStamp)
;
```


## 创建kafka引擎表
```
set allow_experimental_map_type = 1;
drop table default.yinew_detail_kafka;
create table default.yinew_detail_kafka
(
    account           String   ,
    appId             String   ,
    appVersion        String   ,
    carrier           String   ,
    deviceId          String   ,
    deviceType        String   ,
    eventId           String   ,
    ip                String   ,
    latitude          Float64  ,
    longitude         Float64  ,
    netType           String   ,
    osName            String   ,
    osVersion         String   ,
    properties        Map(String,String),
    releaseChannel    String,
    resolution        String,
    sessionId         String,
    timeStamp         Int64
) ENGINE = Kafka('hdp01:9092,hdp02:9092,hdp03:9092','yinew_applog','group1','JSONEachRow');
```

## 创建物化视图
```
create MATERIALIZED VIEW yinew_view TO yinew_detail
as
select
    account        ,
    appId          ,
    appVersion     ,
    carrier        ,
    deviceId       ,
    deviceType     ,
    eventId        ,
    ip             ,
    latitude       ,
    longitude      ,
    netType        ,
    osName         ,
    osVersion      ,
    properties     ,
    releaseChannel  ,
    resolution      ,
    sessionId       ,
    timeStamp
from yinew_detail_kafka
;
```