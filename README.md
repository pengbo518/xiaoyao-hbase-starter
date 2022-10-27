# Hbase-Starter-逍遥出品
* hbase client 2.0.0
* spring boot

## Update
* V1.0
</br> 1.支持操作增删改查
  
## Getting started
- 添加hbase-starter依赖
```xml
<dependency>
    <groupId>com.xiaoyao</groupId>
    <artifactId>xiaoyao-hbase-starter</artifactId>
    <version>Latest Version</version>
</dependency>
```

```yaml
#application.yml
xiaoyao:
  hbase:
    enabled: true
    server:
      server-urls: localhost
      port: 2181
    thread-max: 128
```
- 使用
``` java
    @Autowired
    private HbaseTemplate hbaseTemplate;

    public User getUserByKey(String key){
        User user = hbaseTemplate.get(key, User.class);
    }
```