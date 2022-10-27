package com.xiaoyao.hbase.core;

import com.google.common.collect.Lists;
import com.xiaoyao.hbase.executor.resultset.ResultSetHandler;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.hbase.CompareOperator;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.springframework.util.Assert;
import org.springframework.util.CollectionUtils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.stream.Collectors;

@Slf4j
public class HbaseTemplate implements HbaseOperations {

    private final HbaseFactory hbaseFactory;
    private final ResultSetHandler resultSetHandler;

    public HbaseTemplate(HbaseFactory hbaseFactory, ResultSetHandler resultSetHandler) {
        Assert.notNull(hbaseFactory, "'hbaseFactory' cannot be null");
        Assert.notNull(resultSetHandler, "'resultSetHandler' cannot be null");
        this.hbaseFactory = hbaseFactory;
        this.resultSetHandler = resultSetHandler;
    }

    @Override
    public <T> Boolean save(T data) {
        return this.saveBatch(Lists.newArrayList(data));
    }

    @Override
    public <T> Boolean saveBatch(List<T> list) {
        return this.doSaveBatch(list);
    }

    @Override
    public <T> Boolean deleteByKey(String key, Class<T> clazz) {
        String tableName = this.resultSetHandler.handleResultTableName(clazz);
        return this.execute(tableName, table -> {
            Delete delete = new Delete(Bytes.toBytes(key));
            table.delete(delete);
            return true;
        });
    }

    @Override
    public <T> T get(String key, Class<T> clazz) {
        List<T> list = selectByKeys(Lists.newArrayList(key), clazz);
        if (CollectionUtils.isEmpty(list)) {
            return null;
        }
        return list.get(0);
    }

    @Override
    public <T> List<T> selectByKeys(List<String> keys, Class<T> clazz) {
        String tableName = this.resultSetHandler.handleResultTableName(clazz);
        String familyName = this.resultSetHandler.handleResultFamilyName(clazz);
        return this.execute(tableName, table -> {
            List<Get> getList = new LinkedList<>();
            for (String key : keys) {
                Get get = new Get(Bytes.toBytes(key));
                byte[] family = Bytes.toBytes(familyName);
                get.addFamily(family);
                getList.add(get);
            }
            Result[] results = table.get(getList);
            List<T> rs = new LinkedList<>();
            for (Result result : results) {
                rs.add(resultSetHandler.handleResult(result, clazz));
            }
            return rs;
        });
    }

    @Override
    public <T> List<T> selectListLimit(String lastKey, Integer pageSize, Class<T> clazz) {
        Assert.hasText(lastKey, "lastKey cannot be empty");
        Assert.notNull(pageSize, "pageSize cannot be null");
        String tableName = this.resultSetHandler.handleResultTableName(clazz);
        final Scan scan = new Scan();
        return this.execute(tableName, table -> {
            FilterList filterList = new FilterList(
                    FilterList.Operator.MUST_PASS_ALL);
            Filter pageFilter = new PageFilter(pageSize);
            filterList.addFilter(pageFilter);
            Filter rowFilter = new RowFilter(CompareOperator.GREATER,
                    new BinaryComparator(Bytes.toBytes(lastKey)));
            filterList.addFilter(rowFilter);
            scan.setFilter(filterList);
            ResultScanner scanner = table.getScanner(scan);
            try {
                List<T> rs = new ArrayList<T>();
                for (Result result : scanner) {
                    rs.add(resultSetHandler.handleResult(result, clazz));
                }
                return rs;
            } finally {
                scanner.close();
            }
        });
    }


    public <T> Boolean doSaveBatch(List<T> list) {
        if (CollectionUtils.isEmpty(list)) {
            return true;
        }
        Class<?> clazz = list.get(0).getClass();
        String tableName = this.resultSetHandler.handleResultTableName(clazz);
        this.execute(tableName, table -> {
            List<Put> putList = list.stream().map(item -> resultSetHandler.handleResultPut(item))
                    .collect(Collectors.toList());
            table.put(putList);
            return null;
        });
        return true;
    }

    @Override
    public <T> T execute(String tableName, OperationsCallback<T> callback) {
        Connection connection = this.hbaseFactory.createConnection();
        Table table = null;
        try {
            table = connection.getTable(TableName.valueOf(tableName));
            return callback.doInOperations(table);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }finally {
            try {
                table.close();
            } catch (IOException e) {
                log.error("hbase资源释放失败");
            }

        }
    }

}
