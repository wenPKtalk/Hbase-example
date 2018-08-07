package com.wensir.hbase;

import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Table;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;

public class HBaseConnectionTest {

    @Test
    public void getConnTest() {
        Connection connection = HBaseConnection.getHbaseConn();
        Assert.assertFalse(connection.isClosed());
        HBaseConnection.closeConn();
        Assert.assertTrue(connection.isClosed());
    }

    @Test
    public void getTableTest() {
        try {

            Table table = HBaseConnection.getTable("US_POPULATION");
            Assert.assertEquals(table.getName().getNameAsString(),"US_POPULATION");
            System.out.println(table.getName().getNameAsString());
            table.close();
        } catch (IOException ioe) {
            ioe.printStackTrace();
        }
    }


}