package com.wensir.hbase;

import org.junit.Test;

import static org.junit.Assert.*;

public class HBaseUtilTest {
    @Test
    public void createTable() throws Exception {
        HBaseUtil.createTable("FileTable", new String[]{"fileInfo","saveInfo"});
    }

    @Test
    public void deleteTable() throws Exception {

    }

    @Test
    public void putRow() throws Exception {
    }

    @Test
    public void putRows() throws Exception {
    }

    @Test
    public void getRow() throws Exception {
    }

    @Test
    public void getRow1() throws Exception {
    }

    @Test
    public void getScanner() throws Exception {
    }

    @Test
    public void getScopeScanner() throws Exception {
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