package com.up.cardholderTag.hbase.test;

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
				getHTable(table);
				this.table = table;
			} catch (IOException e) {
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
	
	public ResultScanner scan(String table) {
		ResultScanner rs;
		Scan scan;

		if (!table.equals(this.table)) {
			hTable = null;

			try {
				getHTable(table);
				this.table = table;
			} catch (IOException e) {
				System.err.println("Error accessing HBase table:" + e);
				return null;
			}
		}

		try {
			scan = new Scan();
			rs = hTable.getScanner(scan);

		} catch (IOException e) {
			System.err.println("Error doing scan: " + e);
			return null;

		} catch (ConcurrentModificationException e) {
			return null;
		}

		return rs;
	}

	
	public LoginModel getLoginModel(Result res) {
		LoginModel model = new LoginModel();
		

		System.out.println("User is :" + getValue(res, "user_name"));
		System.out.println("PWD is :" + getValue(res, "user_pwd"));
		
		model.setUserNm(getValue(res, "user_name"));
		model.setUserPwd(getValue(res, "user_pwd"));
		
		return model;
	}
	
	private void getHTable(String table) throws IOException {
		synchronized (tableLock) {
			hTable = new HTable(config, table);
			hTable.setAutoFlush(false);
			hTable.setWriteBufferSize(WRITE_BUFFER_SIZE);
		}
	}
	
	private String getValue(Result res, String qualifier) {
		String str = "";
		if(res != null) {
			str = Bytes.toString(res.getValue(
					"d".getBytes(), qualifier.getBytes()));
		}
		return str;
	}
	
}
