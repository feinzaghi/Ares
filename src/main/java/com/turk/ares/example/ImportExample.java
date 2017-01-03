package com.turk.ares.example;

import com.turk.ares.hbase.HBaseRecordAdd;



public class ImportExample {

	public static void main(String[] args) throws Exception {
		HBaseRecordAdd hbaseimport = HBaseRecordAdd.getInstance("192.168.5.95");
		String tableName = "HBTABLE1";
		String rowkey = "row1";
		String cf = "cf";
		hbaseimport.Add(tableName, rowkey, cf,  "A", "A");
		hbaseimport.Add(tableName, rowkey, cf,  "B", "B");
		hbaseimport.Add(tableName, rowkey, cf,  "C", "C");
		hbaseimport.Add(tableName, rowkey, cf,  "D", "D");
	}
}
