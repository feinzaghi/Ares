package com.turk.ares.hbase;

import java.io.Serializable;
import java.util.ArrayList;

import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;

import com.turk.ares.common.ThreadPool;
import com.turk.ares.common.ZkKafkaConfig;

/**
 * 
 * Hbase Put instance  
 * @author Turk
 *
 */
public class HBaseRecordAdd implements Serializable{

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	static HBaseRecordAdd singleton;
	
	static long lastUsedAdd;
	static long lastUsedIncer;
	
	static long flushInterval = 2000;
	
	static long commitInterval = 60000;
	
	static int addcommitCell = 10000;
	static int incercommitCell = 5000;
	
	
	static FlushThread flushThread;
	
	static int _CounterAdd = 0;
	static int _CounterIncer = 0;
	static ArrayList<HBaseCell> _AddList = new ArrayList<HBaseCell>();
	static ArrayList<HBaseCell> _IncerList = new ArrayList<HBaseCell>();
	
	private static String _zooServer;
	
	private static ThreadPool _threadpoolAdd = new ThreadPool(12);
	private static ThreadPool _threadpoolIncre = new ThreadPool(8);
	
	
	static Object lockerAdd = new Object();
	static Object lockerIncer = new Object();
	static Object locker3 = new Object();
	
	  

	  
	  public HBaseRecordAdd(String zooServer) {
		  _zooServer = zooServer;
		  initialize();
		  _threadpoolAdd = new ThreadPool(10);
		  _threadpoolIncre = new ThreadPool(10);
		  updateLastUsedAdd();
		  updateLastUsedIncer();
	  }
	  
	  

	  
	  public static HBaseRecordAdd getInstance(String zooServer) {
		  
		  if (singleton == null) {
			  singleton = new HBaseRecordAdd(zooServer);
		  }
		  return singleton;
	  }
	  
	  private static void initialize() {
		  updateLastUsedAdd();
	      updateLastUsedIncer();
	      flushThread = new FlushThread(flushInterval);
	      flushThread.start();
	      System.out.println("Flush Thread Start");
	  }
	  
	  
	  private static void updateLastUsedAdd() {
		    lastUsedAdd = System.currentTimeMillis();
		  }
	  
	  private static void updateLastUsedIncer() {
		    lastUsedIncer = System.currentTimeMillis();
		  }
	  
	
	  /**
	   * PUT Add Record
	   * @param tableName
	   * @param rowKey
	   * @param columnFamily
	   * @param key
	   * @param value
	   */
	  public void Add(String tableName,
			  String rowKey, 
			  String columnFamily,
			  String key, 
			  String value) {
		  
		  synchronized (_AddList){ 
			  HBaseCell cell = new HBaseCell();
			  cell.setTableName(tableName);
			  cell.setRowKey(rowKey);
			  cell.setColumnFamily(columnFamily);
			  cell.setKey(key);
			  if(value == null)
				  value = "";
			  cell.setValue(Bytes.toBytes(value));
			  _AddList.add(cell);
			  _CounterAdd ++;
		  }
		  //
	  }
	 
	  
	  public void Add(String tableName,
			  String rowKey, 
			  String columnFamily,
			  String key, 
			  Long value) {
		  
		  synchronized (_AddList){ 
			  HBaseCell cell = new HBaseCell();
			  cell.setTableName(tableName);
			  cell.setRowKey(rowKey);
			  cell.setColumnFamily(columnFamily);
			  cell.setKey(key);
			  cell.setValue(Bytes.toBytes(value));
			  _AddList.add(cell);
			  _CounterAdd ++;
		  }
		  //
	  }
	  
	  
	  /**
	   * 
	   * @param tableName
	   * @param rowKey
	   * @param key cf+column
	   * @param increment
	   */
	  public void incerment(String tableName, String rowKey, 
			  String key, int increment) {
		  
		  incerment(tableName,rowKey,key,(long)increment);
	  }
	  
	  
	  /**
	   * 
	   * @param tableName
	   * @param rowKey
	   * @param key cf+column
	   * @param increment
	   */
	  public void incerment(String tableName, String rowKey, 
			  String key, long increment) {
		  synchronized (_IncerList){
			  HBaseCell cell = new HBaseCell();
			  cell.setTableName(tableName);
			  cell.setRowKey(rowKey);
			 // cell.setColumnFamily(columnFamily);
			  cell.setKey(key);
			  cell.setIncrement(increment);
			  _IncerList.add(cell);
			  _CounterIncer ++;
			  
		  }
	  }
	  

	  protected static class FlushThread extends Thread {
		  long sleepTime;
		  boolean continueLoop = true;

		  public FlushThread(long sleepTime) {
			  this.sleepTime = sleepTime;
		  }

		  @Override
		  public void run() {
			  Logger.getRootLogger().info("FlushThread Run..........*******************");
			  while (continueLoop) {
				  
				  try {
					  Thread.sleep(sleepTime);
				  } catch (InterruptedException e) {
					  Logger.getRootLogger().error("FlushThread ERROR:*********" + e.getMessage(),e);
				  }
				  
				  if(System.currentTimeMillis() - lastUsedAdd > commitInterval 
						  || _CounterAdd >= addcommitCell)
				  {//
					  synchronized (_AddList){
					  	//Logger.getRootLogger().info("******It's time to input [ADD] data!");
					  	HBaseImportQueue queueAdd 
					  		= new HBaseImportQueue(_zooServer);
					  	queueAdd.setWriteCommitLog(true);
					  	
					  	for(HBaseCell cell : _AddList)
					  	{
					  		queueAdd.Add(cell.getTableName(), cell.getRowKey(), 
					  				cell.getColumnFamily(), cell.getKey(), cell.getValue());
					  	}
					  	
//					  	ThreadPool.getInstance().addTask(queueAdd);
					  	_threadpoolAdd.addTask(queueAdd);
					  	updateLastUsedAdd();
					  	Logger.getRootLogger().info("*****HBASE IMPORT --- ThreadPool ******");
					  	Logger.getRootLogger().info(_threadpoolAdd.getRunTaskTime());
					  	
//					  	if(!_zooServer.isEmpty())
//					  	{
//						  	Slave s = new Slave(_zooServer);
//						  	if(_threadpoolAdd.getThreadQueueCount() > 20)
//						  		s.SendAlarmLog("WARN", "Add Thread Pool is Full,Queue=" + _threadpoolAdd.getThreadQueueCount());
//					  	}
					  	
					  	_CounterAdd = 0;
					  	_AddList.clear();
					  }
				  } 
				  
				  if(System.currentTimeMillis() - lastUsedIncer > commitInterval 
						  || _CounterIncer >= incercommitCell)
				  {//
					  synchronized (_IncerList){
					  	//Logger.getRootLogger().info("******It's time to input [INCER] data!");
					  	HBaseImportQueue queueIncer = new HBaseImportQueue(_zooServer);
					  	queueIncer.setWriteCommitLog(true);
					  	
					  	for(HBaseCell cell : _IncerList)
					  	{
					  		queueIncer.incerment(cell.getTableName(), cell.getRowKey(), 
					  				 cell.getKey(), cell.getIncrement());
					  	}
					  	
//					  	ThreadPool.getInstance().addTask(queueIncer);
					  	_threadpoolIncre.addTask(queueIncer);
//					  	_queueIncer = new HBaseImportQueue(_zooServer,_driverServer);
//					  	_queueIncer.setWriteCommitLog(false);
					  	updateLastUsedIncer();
					  	_CounterIncer = 0;
					  	_IncerList.clear();
					  	Logger.getRootLogger().info("*****HBASE IMPORT --- ThreadPool " + _threadpoolIncre.getRunTaskTime());
					  	
//					  	if(!_zooServer.isEmpty() && _threadpoolIncre.getThreadQueueCount() > 10)
//					  	{
//						  	Slave s = new Slave(_zooServer);
//							s.SendAlarmLog("WARN", "Increment Thread Pool is Full,Queue=" + _threadpoolIncre.getThreadQueueCount());
//					  	}
					  }
				  }
			  }
		  }
	  
		  
		  public void stopLoop() {
			  continueLoop = false;
		  }
	  }
}
	  
	  

