package com.turk.ares.hbase;

/**
 * HBASE KV
 * @author Administrator
 *
 */
public class HKeyValue {
	
	public HKeyValue(String columnfamily,String key,byte[] value)
	{
		this._columnfamily = columnfamily;
		this._key = key;
		this._value = value;
	}

	private String _columnfamily = "";
	private String _key = "";
	private byte[] _value = new byte[]{};
	
	private long _time;
	

	public void setKey(String key)
	{
		this._key = key;
	}
	
	public String getKey()
	{
		return this._key;
	}
	
	public void setValue(byte[] value)
	{
		this._value = value;
	}
	
	public byte[] getValue()
	{
		return this._value;
	}
	
	public void setColumnFamily(String columnfamily)
	{
		this._columnfamily = columnfamily;
	}
	
	public String getColumnFamily()
	{
		return this._columnfamily;
	}
	
	public void setTime(long time)
	{
		this._time = time;
	}
	
	public long getTime()
	{
		return this._time;
	}
}
