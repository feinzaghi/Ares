package com.turk.ares.hbase;

import java.io.IOException;
import java.io.Serializable;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Map.Entry;
import java.util.UUID;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Increment;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;

import com.turk.ares.common.KafkaBrokers;
import com.turk.ares.common.Task;
import com.turk.ares.hbase.CounterMap.Counter;
//import com.aotain.kafka.KafkaProducer;
//import com.aotain.mushroom.Slave;


/**
 * HBASE Import Qutue
 * @author Turk
 *
 */
public class HBaseImportQueue extends Task implements Serializable{
	
	/**
	 * 
	 */
	private static final long serialVersionUID = 6392137131909336150L;

	//zookeeper 服务�?
	private String _zooServer;
	
	//mushroom center 服务�?
//	private String _driverServer = "";
	
	private String _brokerList = "";
	
	private Date _Start = new Date();
	
	private boolean _Immediate = false;
	
	HashMap<String, HBaseTable> tableRecordMap = null;
	
	private Connection connection = null;
	
	private String _uid = "";
	
	private HashMap<String,HTable> tablePool = new HashMap<String, HTable>();
	
	static Object locker = new Object();
	
//	public HBaseImportQueue(String zooServer,String driverServer)
//	{
//		UUID uuid = UUID.randomUUID();      
//		_uid = uuid.toString();
//        
//		_zooServer = zooServer;
//		_driverServer = driverServer;
//		initialize();
//	}
	
	public HBaseImportQueue(String zooServer)
	{
		UUID uuid = UUID.randomUUID();      
		_uid = uuid.toString();
        
		_zooServer = zooServer;
//		_driverServer = driverServer;
//		_brokerList = brokerList;
		
		KafkaBrokers kb = KafkaBrokers.getInstance(zooServer, "/kafka_2/brokers/ids");
		_brokerList = kb.getKafkaBrokers();
		
		initialize();
	}
	
	
	private void initialize() {
		 
		  if (tableRecordMap == null) {
			  Logger.getRootLogger().info("Init hbase conn******" + _uid);
	          Configuration hConfig = HBaseConfiguration.create();
	          hConfig.set("hbase.zookeeper.quorum", _zooServer);
	          hConfig.set("hbase.zookeeper.property.clientPort","2181");  
	          //updateLastUsed();
	          
	          try {
	            // establish the connection to the cluster.
	        	//TableName TABLE_NAME = TableName.valueOf(tableName);
	            connection = ConnectionFactory.createConnection(hConfig);
	            // retrieve a handle to the target table.
	            //hTable = connection.getTable(TABLE_NAME);
	            // describe the data we want to write.
	            //Put p = new Put(Bytes.toBytes("someRow"));
	            //p.addColumn(CF, Bytes.toBytes("qual"), Bytes.toBytes(42.0d));
	            // send the data.
	            //table.put(p);
	            tableRecordMap = new HashMap<String, HBaseTable>(); 
	          
	          } catch (IOException e) {
	            throw new RuntimeException(e);
	          }
//	          flushThread = new FlushThread(flushInterval);
//	          flushThread.start();
//	          
//	          System.out.println("Flush Thread Start");
//	          
//	          closerThread = new CloserThread();
//	          closerThread.start();
	          
	          
		  }
	  }
	
	public void Add(String tableName,
			  String rowKey, 
			  String columnFamily,
			  String key, 
			  byte[] value)
	{
//		 if(tableRecordMap == null)
//			  initialize();
		  
		  synchronized (tableRecordMap)
		  {
			  HBaseTable tbl = tableRecordMap.get(tableName);
			  
			  if(tbl == null)
			  {
				  tbl = new HBaseTable(tableName);
				  tableRecordMap.put(tableName, tbl);
			  }
			  
			  tbl.Add(rowKey, columnFamily, key, value);
//			  Logger.getRootLogger().info(String.format("Add %s/%s/%s/%s/%s", 
//					  tableName,rowKey,columnFamily,key,value));
		  }
//		  initialize();
	}
	
	public void incerment(String tableName, String rowKey, 
			  String key, int increment) {
		  synchronized (tableRecordMap)
		  {
			  HBaseTable tbl = tableRecordMap.get(tableName);
			  
			  if(tbl == null)
			  {
				  tbl = new HBaseTable(tableName);
				  tableRecordMap.put(tableName, tbl);
			  }
			  
			  tbl.incerment(rowKey, key, (long)increment);
		  }
		  //initialize();
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
		  synchronized (tableRecordMap)
		  {
			  HBaseTable tbl = tableRecordMap.get(tableName);
			  
			  if(tbl == null)
			  {
				  tbl = new HBaseTable(tableName);
				  tableRecordMap.put(tableName, tbl);
			  }
			  
			  tbl.incerment(rowKey, key, (long)increment);
		  }
		  //initialize();
	  }
	  
	
	  
	@Override
	public Task taskCore() throws Exception {
		// TODO Auto-generated method stub
		//刷入数据�?
		try {
			Logger.getLogger(HBaseImportQueue.class).info("flushToHBase*******************");
			flushToHBase();
		} catch (Exception e) {
			Logger.getLogger(HBaseImportQueue.class).error("flushToHBase ERROR:*********" + e.getMessage(),e);
		} finally {
			close();
		}
		return this;
	}

	@Override
	protected boolean useDb() {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	protected boolean needExecuteImmediate() {
		// TODO Auto-generated method stub
		return _Immediate;
	}
	
	public void setExecuteImmediate(boolean Immediate)
	{
		_Immediate = Immediate;
	}

	@Override
	public void stopTask() {
		// TODO Auto-generated method stub
		
	}

	@Override
	public String info() {
		// TODO Auto-generated method stub
		SimpleDateFormat df1 = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		Date current = new Date();
		String strDate = df1.format(_Start);
		strDate = strDate + "--->" + df1.format(current);
		
		String TableNames = "";
		if(tableRecordMap != null)
		{
			for(String key : tableRecordMap.keySet())
			{
				TableNames = TableNames + key + ",";
			}
		}
			
		
		String info = String.format("%s cost:[%d] uuid[%s] {%s}", 
				strDate,(current.getTime() - _Start.getTime()), _uid, TableNames);
		
		return info;
	}
	
	
	 public void flushToHBase() throws IOException {
//		  synchronized (tableRecordMap) 
		  {
			  if (tableRecordMap == null) {
				  initialize();
			  	}
			  //updateLastUsed();
			 
			  _Start = new Date();

			  
			  
				  for (Entry<String, HBaseTable> entry : tableRecordMap.entrySet()) {
					  

						  
						  Long start = System.currentTimeMillis();
						  
						  int addCount = 0;
						  int increCount = 0;
						  String tableName = entry.getKey();
						  
						  HTable hTable = tablePool.get(tableName);
						  if(hTable == null)
						  {
							  TableName TABLE_NAME = TableName.valueOf(tableName);
							  hTable = (HTable) connection.getTable(TABLE_NAME);
							  tablePool.put(tableName, hTable);
						  }
						  
						  
						  hTable.setAutoFlushTo(false);
						  hTable.setWriteBufferSize(8*1024*1024);
						  
						  HBaseTable rMap = entry.getValue();
						  
						  HashMap<String,ArrayList<HKeyValue>> rows = rMap.getRow();
						  //Logger.getRootLogger().info("Insert Table START " + tableName 
							//	  + ":" + rows.entrySet().size());
						  addCount = rows.entrySet().size();
						  for (Entry<String, ArrayList<HKeyValue>> row : rows.entrySet()) {
							  try
							  {
								  Put put = new Put(Bytes.toBytes(row.getKey()));
								  put.setDurability(Durability.SKIP_WAL);
								  for(HKeyValue kv : row.getValue())
								  {
									  
									  put.addColumn(Bytes.toBytes(kv.getColumnFamily()),
											  Bytes.toBytes(kv.getKey()), 
											  kv.getValue());
									  //list.add(put);
//									  Logger.getRootLogger().info("###HBASE put ROWKEY:" 
//									  + row.getKey() + "/" +  kv.getKey() + "/" + kv.getValue());
								  }
								  
								  hTable.put(put);
								  //addCount++;
							 }
							  catch(Exception ex)
							  {
								  String sLog = String.format("%s TABLE[%s] ROWKEY:%s", 
										  "PUTERROR:*********",tableName,row.getKey());
								  Logger.getLogger(HBaseImportQueue.class).error(sLog,ex);
							  }
						  }
						  
						  rMap.getRow().clear();
						 
						  HashMap<String, CounterMap> counterRows = rMap.getCounterRow();
						  increCount = counterRows.entrySet().size();
						  for (Entry<String, CounterMap> countRow : counterRows.entrySet()) {
							  try
							  {
								  CounterMap pastCounterMap = countRow.getValue();
						          //counterRows.put(countRow.getKey(), new CounterMap());
	
						          Increment increment = new Increment(Bytes.toBytes(countRow.getKey()));
						          increment.setDurability(Durability.SKIP_WAL);
						          
						          boolean hasColumns = false;
						          for (Entry<String, Counter> entry2 : pastCounterMap.entrySet()) {
						        	  
						        	  String key = entry2.getKey();
						        	  String cf = key.split(":",-1)[0];
						        	  String column = key.split(":",-1)[1];
						            increment.addColumn(Bytes.toBytes(cf),
						                Bytes.toBytes(column), entry2.getValue().value);
						            hasColumns = true;						            
						          }
						          if (hasColumns) {
						        	  hTable.increment(increment);
						          }
						          
							  }
							  catch(Exception ex)
							  {
								  Logger.getRootLogger().error("Increment ERROR:*********" + ex.getMessage(),ex);
							  }
							  
							  //
						  }
						  hTable.close();
						  rMap.getCounterRow().clear();
						  
						  
						  if(addCount > 0 || increCount > 0)
						  {
							  
							  Long end = System.currentTimeMillis();
							 
							  String sMsg = String.format("###HBASE TABLENAME:%s Add[%d] Incre[%d] UUID[%s]", 
									  tableName,addCount,increCount, _uid);
							  Logger.getRootLogger().info(sMsg);
							  
							  //Import log -> kafka 
//							  if(!_brokerList.isEmpty())
//							  {//
//								  try
//								  {
//									  Long cost = end - start;  //
//									  String accesstime = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
//									  	.format(new Date(System.currentTimeMillis()));
//									  KafkaProducer produce = new KafkaProducer(_brokerList, _zooServer + ":2181/kafka_2");
//									  //TABLENAME,SERVERNAME,ADDNUM,INCREMENTNUM,COST,UUID
//									  String servername  = "";
//									  try {
//											servername = InetAddress.getLocalHost().getHostName();
//										} catch (UnknownHostException e) {
//											// TODO Auto-generated catch block
//											e.printStackTrace();
//										}
//									  
//									  String msg = String.format("%s|%s|%d|%d|%d|%s|%s", tableName, servername, addCount, 
//											  increCount, cost, _uid, accesstime);
//									  produce.SendMessage("importlog", msg);
//									  produce.CloseProducer();
//								  } catch (Exception ex)
//								  {
//									  String exception = String.format("KafkaProducer ERROR, BrokerList:%s Zookeeper:%s ", 
//											  _brokerList,_zooServer);
//									  Logger.getLogger(HBaseImportQueue.class).error(exception,ex);
//								  }
//							  }
						  }
						  
						  
			  }
				  
		  }
	  }
	 
	 
	  private void close() {
		    if (tablePool.size() > 0) {

//		    	  for(Table hTable : tablePool.values())
//		    	  {
//		    		  if (hTable != null) {
//		    				  try {
//		    					  hTable.close();
//		    				  } catch (IOException e) {
//		    					  // TODO Auto-generated catch block
//		    					  e.printStackTrace();
//		    				  }
//		    				  hTable = null;
//		    		  	}
//		    	  }
		    	  
		    	  if(tableRecordMap!=null)
	    		  {
//		    		  synchronized (tableRecordMap) 
	    			  {
			    		  if(tableRecordMap != null)
			    		  {
			    			  tableRecordMap.clear();
			    			  tableRecordMap = null;
			    		  }
		    		  }
	    		  }
		    	  
		      	}
		      
		    
		    if(connection !=null)
				try {
					Logger.getRootLogger().info("*****hbase conn Close******" + _uid);
					connection.close();
					//connection = null;
				} catch (IOException e) {
					// TODO Auto-generated catch block
					Logger.getLogger(HBaseImportQueue.class).error("Close Hbase Connection",e);
				}
	}

}
