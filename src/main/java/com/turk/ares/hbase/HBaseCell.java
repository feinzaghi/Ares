package com.turk.ares.hbase;

public class HBaseCell {

	private String _tablename = "";
	private String _rowkey = "";
	private String _key = "";
	private String _columnFamily = "";
	private byte[] _value = new byte[]{};
	private long _increment = 0;
	
	public void setTableName(String tablename)
	{
		this._tablename = tablename;
	}
	
	public String getTableName()
	{
		return this._tablename;
	}
	
	public void setRowKey(String rowkey)
	{
		this._rowkey = rowkey;
	}
	
	public String getRowKey()
	{
		return this._rowkey;
	}
	
	public void setKey(String key)
	{
		this._key = key;
	}
	
	public String getKey()
	{
		return this._key;
	}
	
	public void setColumnFamily(String columnFamily)
	{
		this._columnFamily = columnFamily;
	}
	
	public String getColumnFamily()
	{
		return this._columnFamily;
	}
	
	public void setValue(byte[] value)
	{
		this._value = value;
	}
	
	public byte[] getValue()
	{
		return this._value;
	}
	
	public void setIncrement(long increment)
	{
		this._increment = increment;
	}
	
	public long getIncrement()
	{
		return this._increment;
	}
}
