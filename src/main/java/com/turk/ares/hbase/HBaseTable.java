package com.turk.ares.hbase;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Set;
import java.util.Map.Entry;


public class HBaseTable {
	
	public HBaseTable(String tablename)
	{
		this._tableName = tablename;
	}

	private String _tableName;
	private HashMap<String,ArrayList<HKeyValue>> _row = new HashMap<String, ArrayList<HKeyValue>>();
	
	private HashMap<String, CounterMap> rowKeyCounterMap =
		      new HashMap<String, CounterMap>();
	
	
	
	public void setTableName(String tablename)
	{
		this._tableName = tablename;
	}
	
	public String getTableName()
	{
		return this._tableName;
	}
	
	public void setRow(HashMap<String,ArrayList<HKeyValue>> row)
	{
		this._row = row;
	}
	
	public HashMap<String,ArrayList<HKeyValue>> getRow()
	{
		return this._row;
	}
	
	public HashMap<String, CounterMap> getCounterRow()
	{
		return this.rowKeyCounterMap;
	}
	
	public void Add(String rowkey,String columnfamily,String column,
			byte[] value)
	{
		ArrayList<HKeyValue> obj = null;
		if(_row.containsKey(rowkey))
		{
			obj = _row.get(rowkey);
			if(obj == null)
				obj = new ArrayList<HKeyValue>();
		}
		else
		{
			obj = new ArrayList<HKeyValue>();
			_row.put(rowkey, obj);
			
		}
		HKeyValue kv = new HKeyValue(columnfamily,column,value);
		obj.add(kv);
			
	}
	
	/**
	 * 
	 * @param rowkey
	 * @param columnfamily
	 * @param column
	 * @param value
	 * @param time 
	 */
	public void Add(String rowkey,String columnfamily,String column,
			byte[] value,long time)
	{
		ArrayList<HKeyValue> obj = null;
		if(_row.containsKey(rowkey))
		{
			obj = _row.get(rowkey);
			if(obj == null)
				obj = new ArrayList<HKeyValue>();
		}
		else
		{
			obj = new ArrayList<HKeyValue>();
			_row.put(rowkey, obj);
			
		}
		HKeyValue kv = new HKeyValue(columnfamily,column,value);
		kv.setTime(time);
		obj.add(kv);
			
	}
	
	
	 /**
	   * 
	   * @param rowKey
	   * @param key
	   * @param increment
	   */
	  public void incerment(String rowKey, String key, long increment) {
		  
		  synchronized(rowKeyCounterMap)
		  {
			  CounterMap counterMap = rowKeyCounterMap.get(rowKey);
			  if (counterMap == null) {
				  counterMap = new CounterMap();
			      rowKeyCounterMap.put(rowKey, counterMap);
			  }
			  counterMap.increment(key, increment);
		  }
	  }
	  

	public Set<Entry<String,ArrayList<HKeyValue>>> entrySet() {
		// TODO Auto-generated method stub
		return _row.entrySet();
	}
	
}
