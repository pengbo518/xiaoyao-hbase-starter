package com.xiaoyao.hbase.autoconfigure;

import com.xiaoyao.hbase.HbaseProperties;
import com.xiaoyao.hbase.core.DefaultHbaseFactory;
import com.xiaoyao.hbase.core.HbaseFactory;
import com.xiaoyao.hbase.core.HbaseTemplate;
import com.xiaoyao.hbase.executor.resultset.DefaultResultSetHandler;
import com.xiaoyao.hbase.executor.resultset.ResultSetHandler;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration(proxyBeanMethods = false)
@ConditionalOnClass(HbaseTemplate.class)
@EnableConfigurationProperties(HbaseProperties.class)
@ConditionalOnProperty(name = "xiaoyao.hbase.enabled", havingValue = "true")
public class HbaseAutoConfiguration {

    private final HbaseProperties properties;

    public HbaseAutoConfiguration(HbaseProperties properties) {
        this.properties = properties;
    }

    @Bean
    @ConditionalOnMissingBean(HbaseTemplate.class)
    public HbaseTemplate hbaseTemplate(HbaseFactory hbaseFactory, ResultSetHandler resultSetHandler) {
        HbaseTemplate hbaseTemplate = new HbaseTemplate(hbaseFactory, resultSetHandler);
        return hbaseTemplate;
    }

    @Bean
    @ConditionalOnMissingBean(ResultSetHandler.class)
    public ResultSetHandler resultSetHandler() {
        ResultSetHandler defaultResultSetHandler = new DefaultResultSetHandler();
        return defaultResultSetHandler;
    }


    @Bean
    @ConditionalOnMissingBean(HbaseFactory.class)
    public HbaseFactory hbaseProducerFactory() {
        DefaultHbaseFactory factory = new DefaultHbaseFactory(
                this.properties.buildProperties());
        return factory;
    }

}
