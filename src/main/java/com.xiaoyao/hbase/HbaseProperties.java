package com.xiaoyao.hbase;

import com.xiaoyao.hbase.clients.CommonClientConfigs;
import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;

import java.util.HashMap;
import java.util.Map;

@Data
@ConfigurationProperties(HbaseProperties.PREFIX)
public class HbaseProperties {

    public static final String PREFIX = "xiaoyao.hbase";

    private Boolean enabled = false;

    private Map<String, String> properties = new HashMap<>();

    private ZkServer server = new ZkServer();

    private Integer threadMax = 256;

    public Map<String, Object> buildProperties() {
        Map<String, Object> properties = new HashMap<>();
        properties.put(CommonClientConfigs.THREAD_MAX, threadMax);
        properties.putAll(this.server.buildProperties());
        if (this.properties != null) {
            properties.putAll(this.properties);
        }
        return properties;
    }

    @Data
    public static class ZkServer {

        private String serverUrls = "localhost";

        private Integer port = 2181;

        public Map<String, Object> buildProperties() {
            Map<String, Object> properties = new HashMap<>();
            properties.put(CommonClientConfigs.SERVER_URLS, serverUrls);
            properties.put(CommonClientConfigs.ZOOKEEPER_PORT, port);
            return properties;
        }
    }

}
