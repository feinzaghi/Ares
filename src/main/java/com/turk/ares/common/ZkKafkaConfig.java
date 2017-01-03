package com.turk.ares.common;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class ZkKafkaConfig {

//	private String hbase_zkList;
	private String zkConn ;
	private String mushroomServer;
	private String filterIPs;
	private String brokerList;
//	private String topic;
//	private String brokersRoot;
	private String sqlinjection;
	
	public ZkKafkaConfig(String config) {
		Properties prop = new Properties();
		try {
			InputStream in = new FileInputStream(config);
			prop.load(in);  
			mushroomServer = prop.getProperty("mushroomserver").trim();
			zkConn = prop.getProperty("zkConn").trim();
			filterIPs = prop.getProperty("filterIPs").trim();
			brokerList = prop.getProperty("brokerlist").trim();
			
			sqlinjection = prop.getProperty("sqlinjection").trim();
			
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return;
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return;
		}
	}
	
	public String getMushroomServer() {
		return mushroomServer;
	}
	
	public String getSQLInjection() {
		return sqlinjection;
	}
	
	public String getZkConnServer(){
		return zkConn;
	}
	
	public String getFilterIPss(){
		return filterIPs;
	}
	
	public String getBrokerList(){
		return brokerList;
	}
}
