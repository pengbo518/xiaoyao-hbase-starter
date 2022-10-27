package com.xiaoyao.hbase.core;

import org.apache.hadoop.hbase.client.Connection;

public interface HbaseFactory {

    Connection createConnection();
}
