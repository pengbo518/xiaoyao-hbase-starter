package com.xiaoyao.hbase.executor.resultset;

import com.xiaoyao.hbase.annotation.HbaseTable;
import com.xiaoyao.hbase.annotation.TableField;
import com.xiaoyao.hbase.annotation.TableKey;
import org.springframework.util.Assert;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ResultSetWrapper {

    private final String tableName;
    private final String familyName;
    private String keyColumn;
    private String keyField;
    private final List<String> columnNames = new ArrayList<>();
    private final List<String> fieldNames = new ArrayList<>();
    private final Map<String, String> fieldNameMappedColumnNameMap = new HashMap<>();
    private final Map<String, String> columnNameMappedFieldNameMap = new HashMap<>();

    public <T> ResultSetWrapper(Class<T> clazz) {
        HbaseTable table = clazz.getAnnotation(HbaseTable.class);
        Assert.notNull(table, "hbaseTable annotation is null");
        this.tableName = table.tableName();
        this.familyName = table.familyName();

        Field[] fields = clazz.getDeclaredFields();
        for (Field field : fields) {
            String fieldName = field.getName();
            TableKey tableKey = field.getAnnotation(TableKey.class);
            TableField tableField = field.getAnnotation(TableField.class);
            String columnName;
            if (tableKey != null) {
                Assert.isNull(this.keyField, "There cannot be more than one key field");
                this.keyColumn = tableKey.value();
                this.keyField = fieldName;
                continue;
            } else if (tableField != null) {
                columnName = tableField.value();
            } else {
                columnName = field.getName();
            }
            fieldNames.add(fieldName);
            columnNames.add(columnName);
            fieldNameMappedColumnNameMap.put(fieldName, columnName);
            columnNameMappedFieldNameMap.put(columnName, fieldName);
        }
        Assert.hasText(this.keyField, "The key cannot be empty");
    }

    public String getMappedColumnName(String fieldName) {
        String columnName = fieldNameMappedColumnNameMap.get(fieldName);
        return columnName;
    }

    public String getMappedFieldName(String columnName) {
        String fieldName = columnNameMappedFieldNameMap.get(columnName);
        return fieldName;
    }

    public String getFamilyName() {
        return this.familyName;
    }
    public String getKeyField() {
        return this.keyField;
    }
    public String getKeyColumn() {
        return this.keyColumn;
    }
}
