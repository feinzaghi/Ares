package com.turk.ares.common;

import java.util.List;

import org.apache.log4j.Logger;

public class KafkaBrokers {
	
	private static final int SESSION_TIMEOUT = 10000;
	private static KafkaBrokers _instance;
	private static String _brokers = "";
	public KafkaBrokers(String zkServer,String zkPath)
	{
		try {
			ZKControl zkControl = ZKControl.getInstance(zkServer, SESSION_TIMEOUT);
			zkControl.createConnection();
			if(!zkControl.existsPath(zkPath)) {
				zkControl.createPath(zkPath, "");
			}
			
			List<String> brokersList = zkControl.getChild(zkPath);
//			if(brokersList==null)
//				return _brokers;
			
			for(String id : brokersList)
			{
				String brokerJson = zkControl.readData(zkPath + "/" + id);
				KafkaBrokerZK broker = KafkaBrokerZK.getObject(brokerJson);
				_brokers = String.format("%s%s:%d,", _brokers,broker.getHost(),broker.getPort());
			}
			
			if(_brokers.length() > 1)
				_brokers = _brokers.substring(0, _brokers.length() - 1);
		}
		catch(Exception ex)
		{
			Logger.getLogger(KafkaBrokers.class).error("KafkaBrokers getKafkaBrokers",ex);
		}
	}
	
	public static KafkaBrokers getInstance(String zkServer,String zkPath)
	{
		if(_instance == null)
			_instance = new KafkaBrokers(zkServer,zkPath);
		return _instance;
	}

	public String getKafkaBrokers()
	{
		return _brokers;
	}
	
	public static void main(String[] args) throws Exception {
		KafkaBrokers kb = KafkaBrokers.getInstance("192.168.5.68:2181", "/kafka_2/brokers/ids");
		String brokers = kb.getKafkaBrokers();
		System.out.println(brokers);
	}
}
