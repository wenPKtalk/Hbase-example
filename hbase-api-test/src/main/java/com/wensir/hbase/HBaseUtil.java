package com.wensir.hbase;

import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

/**
 * Hbase工具类包含表的创建，删除等操作
 * Hbase的更新操作是先删后插入
 */
public class HBaseUtil {

    /**
     * @param tableName 表名
     * @param cfs       列族数组
     * @return
     */
    public static boolean createTable(String tableName, String[] cfs) {
        //对表操作实例化HbaseAdmin类
        //1.实例化admin
        // 2.判断表是否存在
        try {
            HBaseAdmin admin = (HBaseAdmin) HBaseConnection.getHbaseConn().getAdmin();
            if (admin.tableExists(tableName)) {
                return false;
            }
            HTableDescriptor tableDescriptor = new HTableDescriptor(TableName.valueOf(tableName));
            Arrays.stream(cfs).forEach(cf -> {
                HColumnDescriptor columnDescriptor = new HColumnDescriptor(cf);
                columnDescriptor.setMaxVersions(1);//设置版本
                tableDescriptor.addFamily(columnDescriptor);//表信息添加到HColumnDescriptor上
            });
        } catch (IOException e) {
            e.printStackTrace();
        }

        return true;
    }

    /**
     * 根据表名称删除表
     *
     * @param tableName
     * @return
     */
    public static boolean deleteTable(String tableName) {
        try (HBaseAdmin admin = (HBaseAdmin) HBaseConnection.getHbaseConn().getAdmin()) {
            admin.disableTable(tableName);
            admin.deleteTable(tableName);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return true;
    }

    /**
     * @param tableName
     * @param rowKey    唯一标识
     * @param cfName    列族名
     * @param qualifier 列标识
     * @param data      列数据
     * @return
     */
    public static boolean putRow(String tableName, String rowKey, String cfName, String qualifier,
                                 String data) {

        //1.获取表的实例
        try {
            Table table = HBaseConnection.getTable(tableName);
            Put put = new Put(Bytes.toBytes(rowKey));
            put.addColumn(Bytes.toBytes(cfName), Bytes.toBytes(qualifier), Bytes.toBytes(data));
            table.put(put);
        } catch (IOException e) {
            e.printStackTrace();
        }

        return true;
    }

    /**
     * 插入多行数据
     *
     * @param tableName
     * @param putList
     * @return
     */
    public static boolean putRows(String tableName, List<Put> putList) {
        try {
            Table table = HBaseConnection.getTable(tableName);
            table.put(putList);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return true;
    }

    /**
     * 获取一行数据
     *
     * @return
     */
    public static Result getRow(String tableName, String rowKey) {
        try {
            Table table = HBaseConnection.getTable(tableName);
            Get get = new Get(Bytes.toBytes(rowKey));
            return table.get(get);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }

    /**
     * 根据过滤器获取表数据
     *
     * @param tableName
     * @param rowKey
     * @param filterList
     * @return
     */
    public static Result getRow(String tableName, String rowKey, FilterList filterList) {
        try {
            Table table = HBaseConnection.getTable(tableName);
            Get get = new Get(Bytes.toBytes(rowKey));
            get.setFilter(filterList);
            return table.get(get);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;

    }

    /**
     * 获取全表扫描信息
     *
     * @param tableName
     * @return
     */
    public static ResultScanner getScanner(String tableName) {
        try {
            Table table = HBaseConnection.getTable(tableName);
            Scan scan = new Scan(Bytes.toBytes(tableName));
            scan.setCaching(1000);
            return table.getScanner(scan);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }

    /**
     * 获取指定范围内的表扫描
     *
     * @param tableName
     * @param startRowKey 开始rowkey
     * @param endRowKey   结束rowkey
     * @return
     */
    public static ResultScanner getScopeScanner(String tableName, String startRowKey, String endRowKey) {
        try {
            Table table = HBaseConnection.getTable(tableName);
            Scan scan = new Scan(Bytes.toBytes(tableName));
            scan.setStartRow(Bytes.toBytes(startRowKey));
            scan.setStopRow(Bytes.toBytes(endRowKey));
            return table.getScanner(scan);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;

    }

    /**
     * 根据filterlist 获取一定范围内的扫描数据
     *
     * @param tableName
     * @param startRowKey
     * @param endRowKey
     * @param filterList
     * @return
     */
    public static ResultScanner getScopeScannerByFilter(String tableName, String startRowKey, String endRowKey,
                                                        FilterList filterList) {
        try {
            Table table = HBaseConnection.getTable(tableName);
            Scan scan = new Scan(Bytes.toBytes(tableName));
            scan.setStartRow(Bytes.toBytes(startRowKey));
            scan.setStopRow(Bytes.toBytes(endRowKey));
            scan.setFilter(filterList);
            return table.getScanner(scan);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;

    }

    /**
     * 删除一行数据
     *
     * @param tableName
     * @param rowKey
     * @return
     */
    public static boolean deleteRow(String tableName, String rowKey) {

        try {
            Table table = HBaseConnection.getTable(tableName);
            Delete delete = new Delete(Bytes.toBytes(rowKey));//Hbase数据库只支持二进制的数组
            table.delete(delete);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return true;

    }

    /**
     * 删除列族
     *
     * @param tableName
     * @param cfName
     * @return
     */
    public static boolean deleteColumnFamily(String tableName, String cfName) {
        try {
            HBaseAdmin admin = (HBaseAdmin) HBaseConnection.getHbaseConn().getAdmin();
            admin.deleteColumn(tableName, cfName);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return true;
    }

    /**
     * 删除列标识
     *
     * @param tableName
     * @param rowKey
     * @param qualifier
     * @return
     */
    public static boolean deleteQualifier(String tableName, String rowKey, String cfname, String qualifier) {

        try {
            Table table = HBaseConnection.getTable(tableName);
            Delete delete = new Delete(Bytes.toBytes(rowKey));//Hbase数据库只支持二进制的数组
            delete.addColumn(Bytes.toBytes(cfname), Bytes.toBytes(qualifier));
            table.delete(delete);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return true;

    }

}
