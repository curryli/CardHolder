package com.up.cardholderTag.hbase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.ConcurrentModificationException;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;


public class HBaseService {
	private static final long WRITE_BUFFER_SIZE = 1024 * 1024 * 12;

	private static Configuration config = HBaseConfiguration.create();

	private String table = "";
	private HTable hTable = null;
	private Object tableLock = new Object();
	
	
	public Result get(String table, String key, String columnFamily) {
		Result result;
		Get get;

		if (!table.equals(this.table)) {
			hTable = null;

			try {
				this.table = table;
			} catch (Exception e) {
				System.err.println("Error accessing HBase table:" + e);
				return null;
			}
		}

		try {
			get = new Get(Bytes.toBytes(key));
			get.addFamily(columnFamily.getBytes());
			result = hTable.get(get);

		} catch (Exception e) {
			System.err.println("HBaseService: Error doing get: " + e);
			return null;
		}

		return result;
	}
	
	
	public static void main(String args[]){
		HBaseService app = new HBaseService();
		System.out.print(app.toString());
	}
	
	
}
