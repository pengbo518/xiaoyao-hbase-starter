package com.xiaoyao.hbase.core;

import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

@Slf4j
public class DefaultHbaseFactory implements HbaseFactory {

    private final Map<String, Object> configs;

    private volatile Connection connection;

    public DefaultHbaseFactory(Map<String, Object> configs) {
        this.configs = new HashMap<>(configs);
    }

    @Override
    public Connection createConnection() {
        return this.doCreateConnection();
    }

    private Connection doCreateConnection() {
        if (this.connection == null) {
            Configuration configuration = new Configuration();
            for(Map.Entry<String, Object> map : this.configs.entrySet()){
                configuration.set(map.getKey(), map.getValue() + "");
            }
            synchronized (this) {
                if (this.connection == null) {
                    try {
                        ThreadPoolExecutor poolExecutor = new ThreadPoolExecutor(200, Integer.MAX_VALUE, 60L, TimeUnit.SECONDS, new SynchronousQueue<Runnable>());
                        this.connection = ConnectionFactory.createConnection(configuration, poolExecutor);
                    } catch (Exception e) {
                        throw new RuntimeException("create connection error", e);
                    }
                }
                return this.connection;
            }
        }
        return this.connection;
    }
}
