package com.wensir.hbase;

import com.sun.javafx.font.freetype.HBGlyphLayout;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.*;

public class HBaseUtilTest {
    /**
     * 创建表
     * @throws Exception
     */
    @Test
    public void createTable() throws Exception {
        HBaseUtil.createTable("FileTable", new String[]{"fileInfo", "saveInfo"});
        HBaseUtil.createTable("FileTable2", new String[]{"fileInfo2", "saveInfo2"});
    }

    @Test
    public void deleteTable() throws Exception {
        HBaseUtil.deleteTable("FileTable2");
    }

    /**
     * 加数据
     * @throws Exception
     */
    @Test
    public void putRow() throws Exception {

        HBaseUtil.putRow("FileTable", "rwoKey1", "fileInfo", "name", "file.txt");
        HBaseUtil.putRow("FileTable", "rwoKey1", "fileInfo", "type", "text");
        HBaseUtil.putRow("FileTable", "rwoKey1", "fileInfo", "size", "1024");
        HBaseUtil.putRow("FileTable", "rwoKey1", "saveInfo", "creator", "wensir");
        HBaseUtil.putRow("FileTable", "rwoKey2", "fileInfo", "name", "file2.jpg");
        HBaseUtil.putRow("FileTable", "rwoKey2", "fileInfo", "type", "jpg");
        HBaseUtil.putRow("FileTable", "rwoKey2", "fileInfo", "size", "1024");
        HBaseUtil.putRow("FileTable", "rwoKey2", "saveInfo", "creator", "wensir");


    }

    @Test
    public void putRows() throws Exception {
        List puts = new ArrayList();
        puts.add(new Put(Bytes.toBytes("rowKey3")).addColumn(Bytes.toBytes("fileInfo"),Bytes.toBytes("name") , Bytes.toBytes("file3.md")));
        puts.add(new Put(Bytes.toBytes("rowKey3")).addColumn(Bytes.toBytes("fileInfo"),Bytes.toBytes("type") , Bytes.toBytes("md")));
        puts.add(new Put(Bytes.toBytes("rowKey3")).addColumn(Bytes.toBytes("fileInfo"),Bytes.toBytes("size") , Bytes.toBytes("3072")));
        puts.add(new Put(Bytes.toBytes("rowKey3")).addColumn(Bytes.toBytes("saveInfo"),Bytes.toBytes("creator") , Bytes.toBytes("wensir")));

        HBaseUtil.putRows("FileTable",puts);
    }

    /**
     * 获取行数据
     * @throws Exception
     */
    @Test
    public void getRow() throws Exception {
        Result result = HBaseUtil.getRow("FileTable", "rwoKey1");
        if (null != result) {
            System.out.println("rowKey=" + Bytes.toString(result.getRow()));
            System.out.println("fileName=" + Bytes.toString(result.getValue(Bytes.toBytes("fileInfo"), Bytes.toBytes("name"))));
            System.out.println("fileName=" + Bytes.toString(result.getValue(Bytes.toBytes("saveInfo"), Bytes.toBytes("creator"))));

        }
    }

    @Test
    public void getRow1() throws Exception {
    }

    @Test
    public void getScanner() throws Exception {
    }

    @Test
    public void getScopeScanner() throws Exception {
        ResultScanner resultScanner = HBaseUtil.getScopeScanner("FileTable", "rwoKey1", "rwoKey2");
        if (null != resultScanner) {
            resultScanner.forEach(result -> {
                System.out.println("rowKey=" + result.getRow());
                System.out.println("fileName="+Bytes.toString(result.getValue(Bytes.toBytes("fileInfo"),Bytes.toBytes("name"))));
            });
        }
    }

    @Test
    public void getScopeScannerByFilter() throws Exception {
    }

    @Test
    public void deleteRow() throws Exception {
    }

    @Test
    public void deleteColumnFamily() throws Exception {
    }

    @Test
    public void deleteQualifier() throws Exception {
    }

}