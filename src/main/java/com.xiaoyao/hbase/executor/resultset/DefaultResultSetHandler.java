package com.xiaoyao.hbase.executor.resultset;

import com.xiaoyao.hbase.annotation.HbaseTable;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.springframework.util.Assert;
import org.springframework.util.ReflectionUtils;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.Map;
import java.util.NavigableMap;

@Slf4j
public class DefaultResultSetHandler implements ResultSetHandler {

    private <T> HbaseTable getHbaseTable(Class<T> clazz) {
        HbaseTable table = clazz.getAnnotation(HbaseTable.class);
        Assert.notNull(table, "hbaseTable annotation is null");
        return table;
    }

    @Override
    public <T> String handleResultTableName(Class<T> clazz) {
        HbaseTable table = this.getHbaseTable(clazz);
        String tableName = table.tableName();
        return tableName;
    }

    @Override
    public <T> String handleResultFamilyName(Class<T> clazz) {
        HbaseTable table = this.getHbaseTable(clazz);
        String familyName = table.familyName();
        return familyName;
    }

    @Override
    public <T> T handleResult(Result result, Class<T> clazz) {
        T t = null;
        try {
            t = clazz.newInstance();
        } catch (Exception e) {
            log.error("instance class is error", e);
        }
        ResultSetWrapper resultSetWrapper = new ResultSetWrapper(clazz);

        NavigableMap<byte[], byte[]> familyMap = result.getFamilyMap(Bytes.toBytes(resultSetWrapper.getFamilyName()));
        if (familyMap == null) {
            return null;
        }
        for (Map.Entry<byte[], byte[]> entry : familyMap.entrySet()) {
            String fieldName = new String(entry.getKey());
            String cellValue = new String(entry.getValue());
            String mappedFieldName = resultSetWrapper.getMappedFieldName(fieldName);
            if (StringUtils.isEmpty(mappedFieldName)) {
                continue;
            }
            this.fillProperties(t, mappedFieldName, cellValue);
        }
        String keyValue = new String(result.getRow());
        this.fillProperties(t, resultSetWrapper.getKeyField(), keyValue);
        return t;
    }

    @Override
    public <T> Put handleResultPut(T data) {
        Class<?> clazz = data.getClass();
        ResultSetWrapper resultSetWrapper = new ResultSetWrapper(clazz);
        String familyName = resultSetWrapper.getFamilyName();
        String keyField = resultSetWrapper.getKeyField();
        String keyValue = this.getPropertiesValue(data, keyField);
        Put put = new Put(Bytes.toBytes(keyValue));
        for (Field field : clazz.getDeclaredFields()) {
            String mappedColumnName = resultSetWrapper.getMappedColumnName(field.getName());
            if (mappedColumnName == null) {
                continue;
            }
            String value = this.getPropertiesValue(data, field.getName());
            if (value == null) {
                continue;
            }
            put.addColumn(Bytes.toBytes(familyName), Bytes.toBytes(mappedColumnName), Bytes.toBytes(value));
        }
        return put;
    }

    public <T> String getPropertiesValue(T t, String fieldName) {
        String methodName = "get" + StringUtils.capitalize(fieldName);
        Method method = ReflectionUtils.findMethod(t.getClass(), methodName);
        try {
            Object o = ReflectionUtils.invokeMethod(method, t);
            if (o == null) {
                return null;
            }
            return o.toString();
        } catch (Exception e) {
            throw new RuntimeException(String.format("class %s %s() method is undefined",
                    t.getClass().getName(), methodName), e);
        }
    }
    public <T> void fillProperties(T t, String mappedFieldName, String cellValue) {
        String methodName = "set" + StringUtils.capitalize(mappedFieldName);
        Method method = ReflectionUtils.findMethod(t.getClass(), methodName, String.class);
        try {
            ReflectionUtils.invokeMethod(method, t, cellValue);
        } catch (Exception e) {
            throw new RuntimeException(String.format("class %s %s(java.lang.String) method is undefined",
                    t.getClass().getName(), methodName), e);
        }
    }

}
