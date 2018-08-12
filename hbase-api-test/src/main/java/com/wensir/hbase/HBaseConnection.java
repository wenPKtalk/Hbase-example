package com.wensir.hbase;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Table;

import java.io.IOException;

public class HBaseConnection {
    //单例链接
    private static final HBaseConnection INSTANCE = new HBaseConnection();
    //配置属性
    private static Configuration configuration;
    //链接变量
    private static Connection connection;

    //构造器
    private HBaseConnection() {

        try {
            //校验configuration是否为空
            if (null == configuration) {
                configuration = HBaseConfiguration.create();
                //客户端链接hbase只需要zk的地址
                configuration.set("hbase.zookeeper.quorum", "localhost:2181");

            }

        } catch (Exception e) {
            System.out.println(e.getStackTrace());

        }
    }

    //获取链接
    private Connection getConnection() {
        //如果connection为空或者connection处于关闭状态
        if (null == connection || connection.isClosed()) {
            try {
                //链接工厂创建链接
                connection = ConnectionFactory.createConnection(configuration);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        return connection;
    }

    /**
     * 获取实例
     * @return
     */
    public static Connection getHbaseConn() {
        return INSTANCE.getConnection();
    }

    /**
     *获取表名称
     */
    public static Table getTable(String tableName) throws IOException {
        return INSTANCE.getConnection().getTable(TableName.valueOf(tableName));
    }

    /**
     * 关闭链接
     */
    public static void closeConn() {
        if (null != connection) {
            try {
                connection.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

}
