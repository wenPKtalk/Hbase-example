package com.wensir.hbase;

import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.filter.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;

public class HBaseFilterTest {

    @Test
    private void rowFilterTest() {
        Filter filter = new RowFilter(CompareFilter.CompareOp.EQUAL, new BinaryComparator(Bytes.toBytes("rowkey1")));

        FilterList filterList = new FilterList(FilterList.Operator.MUST_PASS_ALL, Arrays.asList(filter));

        ResultScanner scanner = HBaseUtil.getScopeScannerByFilter("FileTable", "rowkey1", "rowkey2", filterList);
        if (null != scanner) {
            scanner.forEach(s -> {
                System.out.println("rowkey=" + Bytes.toString(s.getRow()));
                System.out.println("fileName=" + Bytes.toString(s.getValue(Bytes.toBytes("fileInfo"), Bytes.toBytes("name"))));

            });

        }
        scanner.close();

    }

    @Test
    private void preFixFilterTest() {

        Filter filter = new PrefixFilter(Bytes.toBytes("rowkey2"));

        FilterList filterList = new FilterList(FilterList.Operator.MUST_PASS_ALL, Arrays.asList(filter));

        ResultScanner scanner = HBaseUtil.getScopeScannerByFilter("FileTable", "rowkey1", "rowkey2", filterList);
        if (null != scanner) {
            scanner.forEach(s -> {
                System.out.println("rowkey=" + Bytes.toString(s.getRow()));
                System.out.println("fileName=" + Bytes.toString(s.getValue(Bytes.toBytes("fileInfo"), Bytes.toBytes("name"))));

            });

        }
        scanner.close();

    }

    @Test
    public void keyOnlyFilterTest() {

        Filter filter = new KeyOnlyFilter(true);

        FilterList filterList = new FilterList(FilterList.Operator.MUST_PASS_ALL, Arrays.asList(filter));

        ResultScanner scanner = HBaseUtil.getScopeScannerByFilter("FileTable", "rowkey1", "rowkey2", filterList);
        if (null != scanner) {
            scanner.forEach(s -> {
                System.out.println("rowkey=" + Bytes.toString(s.getRow()));
                System.out.println("fileName=" + Bytes.toString(s.getValue(Bytes.toBytes("fileInfo"), Bytes.toBytes("name"))));

            });

        }
        scanner.close();
    }

    @Test
    public void columnPrefixFiltetTest() {

        Filter filter = new ColumnPrefixFilter(Bytes.toBytes("name"));

    }

}
