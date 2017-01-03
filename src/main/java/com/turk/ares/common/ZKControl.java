package com.turk.ares.common;

import java.io.IOException; 
import java.util.List;
import java.util.concurrent.CountDownLatch; 

import org.apache.log4j.Logger;
import org.apache.zookeeper.CreateMode; 
import org.apache.zookeeper.KeeperException; 
import org.apache.zookeeper.WatchedEvent; 
import org.apache.zookeeper.Watcher; 
import org.apache.zookeeper.Watcher.Event.KeeperState; 
import org.apache.zookeeper.ZooDefs.Ids; 
import org.apache.zookeeper.ZooKeeper; 
 


/**
 * zookeeper 
 * @author Administrator
 *
 */
public class ZKControl implements Watcher {

	private static final int SESSION_TIMEOUT = 10000; 
    private static final String CONNECTION_STRING = "192.168.5.97,192.168.5.98,192.168.5.95:2181"; 
    private static final String ZK_PATH = "/mushroom"; 
    private ZooKeeper zk = null; 
     
    private CountDownLatch connectedSemaphore = new CountDownLatch( 1 ); 
    
    private static ZKControl _instance = null;
    
    public static ZKControl getInstance(String connectString, int sessionTimeout)
    {
    	if(_instance == null)
    		_instance = new ZKControl(connectString,sessionTimeout);
    	return _instance;
    }
 
    public ZKControl(String connectString, int sessionTimeout)
    {
    	try { 
        	if(!connectString.contains("2181"))
        		connectString = connectString + ":2181";
            zk = new ZooKeeper( connectString, sessionTimeout, this ); 
            connectedSemaphore.await(); 
        } catch ( InterruptedException e ) { 
            System.out.println( "连接创建失败，发�? InterruptedException" ); 
            e.printStackTrace(); 
        } catch ( IOException e ) { 
            System.out.println( "连接创建失败，发�? IOException" ); 
            e.printStackTrace(); 
        } 
    }
    
    /** 
     * 创建ZK连接 
     * @param connectString  ZK服务器地�?列表 
     * @param sessionTimeout   Session超时时间 
     */ 
    public void createConnection() { 
//        this.releaseConnection(); 
        
    } 
 
    /** 
     * 关闭ZK连接 
     */ 
    public void releaseConnection() { 
    	
        //if ( !ObjectUtil.isBlank( this.zk ) ) { 
            try { 
                this.zk.close(); 
            } catch ( InterruptedException e ) { 
                // ignore 
                e.printStackTrace(); 
            } 
        //} 
    } 
    
    /*
    *<b>function:</b>获取节点信息 
    *@author cuiran 
    *@createDate 2013-01-16 15:17:22 
    *@param path 
    *@throws KeeperException 
    *@throws InterruptedException 
    */  
    public List<String> getChild(String path) throws KeeperException, InterruptedException{     
       try{  
           List<String> list=this.zk.getChildren(path, false);  
           if(list.isEmpty()){  
               return null;
           }else{  
               return list;
           }  
       }catch (KeeperException.NoNodeException e) {  
           // TODO: handle exception  
            throw e;     
 
       }  
   }  
 
    /** 
     *  创建节点 
     * @param path 节点path 
     * @param data 初始数据内容 
     * @return 
     * @throws InterruptedException 
     * @throws KeeperException 
     */ 
    public boolean createPath( String path, String data ) throws KeeperException, InterruptedException { 
//        try { 
//            System.out.println( "节点创建成功, Path: " 
                    this.zk.create( path,  data.getBytes(),Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT ); 
//                    + ", content: " + data ); 
//        } catch ( KeeperException e ) { 
//            System.out.println( "节点创建失败，发生KeeperException" ); 
//            e.printStackTrace(); 
//        } catch ( InterruptedException e ) { 
//            System.out.println( "节点创建失败，发�? InterruptedException" ); 
//            e.printStackTrace(); 
//        } 
        return true; 
    } 
    
    /** 
     *  �?查节点是否存�?
     * @param path 节点path 
     * @return 
     * @throws InterruptedException 
     * @throws KeeperException 
     */
    public boolean existsPath(String path) throws KeeperException, InterruptedException  {
    	
    	try {
			return this.zk.exists(path, this) == null ? false : true;
		} catch (KeeperException e) {
			// TODO Auto-generated catch block
			Logger.getLogger(ZKControl.class).error("ZKControl ERROR zkServer:" 
					+ this.zk,e);
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			Logger.getLogger(ZKControl.class).error("ZKControl ERROR zkServer:"
					+ this.zk,e);
		}
    	return false;
    }
 
    /** 
     * 读取指定节点数据内容 
     * @param path 节点path 
     * @return 
     * @throws InterruptedException 
     * @throws KeeperException 
     */ 
    public String readData( String path ) throws KeeperException, InterruptedException { 
//        try { 
//            System.out.println( "获取数据成功，path�?" + path ); 
            return new String( this.zk.getData( path, false, null ) ); 
//        } catch ( KeeperException e ) { 
//            System.out.println( "读取数据失败，发生KeeperException，path: " + path  ); 
//            e.printStackTrace(); 
//            return ""; 
//        } catch ( InterruptedException e ) { 
//            System.out.println( "读取数据失败，发�? InterruptedException，path: " + path  ); 
//            e.printStackTrace(); 
//            return ""; 
//        } 
    } 
 
    /** 
     * 更新指定节点数据内容 
     * @param path 节点path 
     * @param data  数据内容 
     * @return 
     * @throws InterruptedException 
     * @throws KeeperException 
     */ 
    public boolean writeData( String path, String data ) throws KeeperException, InterruptedException { 
//        try { 
//            System.out.println( "更新数据成功，path�?" + path + ", stat: " + 
    		this.zk.setData( path, data.getBytes(), -1 ) ; 
//        } catch ( KeeperException e ) { 
//            System.out.println( "更新数据失败，发生KeeperException，path: " + path  ); 
//            e.printStackTrace(); 
//        } catch ( InterruptedException e ) { 
//            System.out.println( "更新数据失败，发�? InterruptedException，path: " + path  ); 
//            e.printStackTrace(); 
//        } 
        return true; 
    } 
 
    /** 
     * 删除指定节点 
     * @param path 节点path 
     * @throws KeeperException 
     * @throws InterruptedException 
     */ 
    public void deleteNode( String path ) throws InterruptedException, KeeperException { 
//        try { 
            this.zk.delete( path, -1 ); 
//            System.out.println( "删除节点成功，path�?" + path ); 
//        } catch ( KeeperException e ) { 
//            System.out.println( "删除节点失败，发生KeeperException，path: " + path  ); 
//            e.printStackTrace(); 
//        } catch ( InterruptedException e ) { 
//            System.out.println( "删除节点失败，发�? InterruptedException，path: " + path  ); 
//            e.printStackTrace(); 
//        } 
    } 
 
    public static void main( String[] args ) throws KeeperException, InterruptedException { 
 
    	ZKControl sample = ZKControl.getInstance( CONNECTION_STRING, SESSION_TIMEOUT); 
        sample.createConnection(); 
        System.out.println(sample.existsPath(ZK_PATH));
//        if ( sample.createPath( ZK_PATH, "我是节点初始内容" ) ) { 
//            System.out.println(); 
//            System.out.println( "数据内容: " + sample.readData( ZK_PATH ) + "\n" ); 
//            sample.writeData( ZK_PATH, "更新后的数据2" ); 
//            System.out.println( "数据内容: " + sample.readData( ZK_PATH ) + "\n" ); 
//            sample.deleteNode( ZK_PATH ); 
            System.out.println(sample.readData(ZK_PATH));
//        } 
 
        sample.releaseConnection(); 
    } 
 
    /** 
     * 收到来自Server的Watcher通知后的处理�? 
     */ 
    @Override 
    public void process( WatchedEvent event ) { 
        System.out.println( "收到事件通知�?" + event.getState() +"\n"  ); 
        if ( KeeperState.SyncConnected == event.getState() ) { 
            connectedSemaphore.countDown(); 
        } 
 
    } 

}
