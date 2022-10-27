package com.xiaoyao.hbase.core;

import org.apache.hadoop.hbase.client.Table;

import java.io.IOException;
import java.util.List;

public interface HbaseOperations {

    <T> Boolean save(T data);

    <T> Boolean saveBatch(List<T> list);

    <T> Boolean deleteByKey(String key, Class<T> clazz);

    <T> T get(String key, Class<T> clazz);

    <T> List<T> selectByKeys(List<String> keys, Class<T> clazz);

    <T> List<T> selectListLimit(String lastKey, Integer pageSize, Class<T> clazz);

    <T> T execute(String tableName, OperationsCallback<T> callback);

    interface OperationsCallback<T> {
        T doInOperations(Table table) throws IOException;
    }
}
