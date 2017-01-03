package com.turk.ares.common;


import net.sf.json.JSONObject;


public class KafkaBrokerZK {
	
	//{"jmx_port":9393,"timestamp":"1477764059006","host":"hadoop-r720-3","version":1,"port":9092}
	
	private int _jmx_port;
	
	public int getJmxPort()
	{
		return _jmx_port;
	}
	
	public void setJmxPort(int jmx_port)
	{
		_jmx_port = jmx_port;
	}
	
	private String _timestamp;
	
	public String getTimestamp()
	{
		return _timestamp;
	}
	
	public void setTimestamp(String timestamp)
	{
		_timestamp = timestamp;
	}
	
	private String _host;
	
	public String getHost()
	{
		return _host;
	}
	
	public void setHost(String host)
	{
		_host = host;
	}
	
	private int _version;
	
	public int getVersion()
	{
		return _version;
	}
	
	public void setVersion(int version)
	{
		_version = version;
	}
	
	private int _port;
	
	public int getPort()
	{
		return _port;
	}
	
	public void setPort(int port)
	{
		_port = port;
	}
	
	private String[] _endpoints;
	
	public String[] getEndpoints()
	{
		return _endpoints;
	}
	
	public void setEndpoints(String[] endpoints)
	{
		_endpoints = endpoints;
	}
	
	public static KafkaBrokerZK getObject(String json)
	{
		try
		{
			JSONObject jsonObject = JSONObject.fromObject(json); 
			KafkaBrokerZK bean = (KafkaBrokerZK)JSONObject.toBean(jsonObject,KafkaBrokerZK.class);
			return bean;
		}
		catch(Exception ex)
		{
			System.err.println(ex);
			return null;
		}
	}

}
