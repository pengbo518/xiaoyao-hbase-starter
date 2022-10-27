package com.xiaoyao.hbase.executor.resultset;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;

public interface ResultSetHandler {

    <T> String handleResultTableName(Class<T> clazz);

    <T> String handleResultFamilyName(Class<T> clazz);

    <T> T handleResult(Result result, Class<T> clazz);

    <T> Put handleResultPut(T data);
}
