package com.turk.ares.common;
import java.util.Collections;
import java.util.Date;
import java.util.LinkedList;
import java.util.List;

import org.apache.log4j.Logger;


/**
 * 线程�?
 * @author Administrator
 *
 */
public class ThreadPool {
    private static Logger logger = Logger.getRootLogger();
    private static Logger taskLogger = Logger.getRootLogger();

    private static boolean debug = taskLogger.isDebugEnabled();
    // private static boolean debug = taskLogger.isInfoEnabled();
    /* 单例 */
    private static ThreadPool instance = ThreadPool.getInstance();

    public static final int SYSTEM_BUSY_TASK_COUNT = 150;
    /* 默认池中线程�? */
    public int worker_num = 10;
    
    private String threadName = "Common Thread Pool";
    
    /* 已经处理的任务数 */
    private static int taskCounter = 0;

    public static boolean systemIsBusy = false;

    private List<Task> taskQueue = Collections
            .synchronizedList(new LinkedList<Task>());
    /* 池中的所有线�? */
    public PoolWorker[] workers;

    private ThreadPool() {
        workers = new PoolWorker[5];
        for (int i = 0; i < workers.length; i++) {
            workers[i] = new PoolWorker(i);
        }
    }

    public ThreadPool(int pool_worker_num) {
        worker_num = pool_worker_num;
        workers = new PoolWorker[worker_num];
        for (int i = 0; i < workers.length; i++) {
            workers[i] = new PoolWorker(i);
        }
    }

    public static synchronized ThreadPool getInstance() {
        if (instance == null)
            return new ThreadPool(12);
        return instance;
    }
    
    public static synchronized ThreadPool getInstance(int MaxThreadNum) {
        if (instance == null)
            return new ThreadPool(MaxThreadNum);
        return instance;
    }
    /**
    * 增加新的任务
    * 每增加一个新任务，都要唤醒任务队�?
    * @param newTask
    */
    public void addTask(Task newTask) {
        synchronized (taskQueue) {
        	if(taskCounter > 10000000)
        		taskCounter = 0;
            newTask.setTaskId(++taskCounter);
            newTask.setSubmitTime(new Date());
            
            taskQueue.add(newTask);
            /* 唤醒队列, �?始执�? */
            taskQueue.notifyAll();
        }
        if(newTask.getWriteCommitLog())
	        logger.debug("Submit Task<" + newTask.getTaskId() + ">: "
	                + newTask.info());
    }
    /**
    * 批量增加新任�?
    * @param taskes
    */
    public void batchAddTask(Task[] taskes) {
        if (taskes == null || taskes.length == 0) {
            return;
        }
        synchronized (taskQueue) {
            for (int i = 0; i < taskes.length; i++) {
                if (taskes[i] == null) {
                    continue;
                }
                taskes[i].setTaskId(++taskCounter);
                taskes[i].setSubmitTime(new Date());
                taskQueue.add(taskes[i]);
            }
            /* 唤醒队列, �?始执�? */
            taskQueue.notifyAll();
        }
        for (int i = 0; i < taskes.length; i++) {
            if (taskes[i] == null) {
                continue;
            }
            if(taskes[i].getWriteCommitLog())
	            logger.debug("Submit Task<" + taskes[i].getTaskId() + ">: "
	                    + taskes[i].info());
        }
    }
    /**
    * 线程池信�?
    * @return
    */
    public String getInfo() {
        StringBuffer sb = new StringBuffer();
        sb.append("\nTask ["+threadName+"] Queue Size:" + taskQueue.size());
        for (int i = 0; i < workers.length; i++) {
            sb.append("\nWorker " + i + " is "
                    + ((workers[i].isWaiting()) ? "Waiting." : "Running."));
        }
        return sb.toString();
    }
    
    /**
     * 当前已激活的任务�?
     * @return
     */
    public int ActiveTaskCount()
    {
    	int count = 0;
    	 for (int i = 0; i < workers.length; i++) {
             if(!workers[i].isWaiting())
             {
            	 count++;
             }
         }
    	 return count;
    }
    
    public String getRunTask()
    {
    	//换行�?
    	String lineSeparator = "\r\n";
    	
    	StringBuffer sb = new StringBuffer();

        sb.append("Task Queue Size:" + taskQueue.size() + lineSeparator);
        for (int i = 0; i < workers.length; i++) {
        	if(!workers[i].isWaiting())
        	{
        		sb.append("TID["+ workers[i].ThisTask().getTaskId() +"]"+"Worker " + workers[i].workername + lineSeparator);
        	}
        }
        return sb.toString();
    }
    
    
    public String getRunTaskTime()
    {
    	//换行�?
    	String lineSeparator = "\r\n";
    	
    	StringBuffer sb = new StringBuffer();
        
        for (int i = 0; i < workers.length; i++) {
        	if(!workers[i].isWaiting() && workers[i].task != null)
        	{
        		sb.append("TID["+ workers[i].ThisTask().getTaskId() +"]"+"Worker " + workers[i].task.info() + lineSeparator);
        	}
        }
        sb.append("Task Waiting Queue Size:" + taskQueue.size());
        
        return sb.toString();
    }
    
    /**
     * 当前线程队列�?
     * @return
     */
    public int getThreadQueueCount()
    {
    	return taskQueue.size();
    }
    
    public String getThreadSummary()
    {
    	StringBuffer sb = new StringBuffer();
    	sb.append("Task Queue Size: " + taskQueue.size());
    	return sb.toString();
    }
    /**
    * �?毁线程池
    */
    public synchronized void destroy() {
        for (int i = 0; i < workers.length; i++) {
            workers[i].stopWorker();
            workers[i] = null;
        }
        taskQueue.clear();
    }
    
    /**
     * 强制执行�?次任务终止，此处不是直接�?死任务线程，而是通过放方法让正在运行的任务跳�?
     * �?要单独实现继承Task类的stop方法
     */
     public synchronized void stoptask() {
         for (int i = 0; i < taskQueue.size(); i++) {
        	 taskQueue.get(i).stopTask();
         }
         
         for (int i = 0; i < workers.length; i++) {
         	if(!workers[i].isWaiting())
         	{
	        	 logger.debug("stop worker " + workers[i].workername);
	             if(workers[i].ThisTask() != null)
	             {
	            	 workers[i].ThisTask().stopTask();
	             }
         	}
         }
     }
     
     /**
      * 强制执行�?次任务终止，此处不是直接�?死任务线程，而是通过放方法让正在运行的任务跳�?
      * �?要单独实现继承Task类的stop方法
      */
      public synchronized Task getTask(int taskid) {
          for (int i = 0; i < workers.length; i++) {
          	if(!workers[i].isWaiting())
          	{
 	        	 
 	             if(workers[i].ThisTask() != null && workers[i].ThisTask().getTaskId() == taskid)
 	             {
 	            	 logger.debug("stop worker " + workers[i].workername);
 	            	 return workers[i].ThisTask();
 	             }
          	}
          }
          return null;
      }
      
      public void setThreadName(String name)
      {
    	  threadName = name;
      }
    
    

    /**
    * 池中工作线程
    * 
    * @author obullxl
    */
    private class PoolWorker extends Thread {
        private int index = -1;
        /* 该工作线程是否有�? */
        private boolean isRunning = true;
        /* 该工作线程是否可以执行新任务 */
        private boolean isWaiting = true;
        
        private String workername = "";
        
        private Task task = null;

        public PoolWorker(int index) {
            this.index = index;
            start();
        }

        public void stopWorker() {
            this.isRunning = false;
        }

        public boolean isWaiting() {
            return this.isWaiting;
        }
        
        public String WorkerName()
        {
        	return this.workername;
        }
        
        public Task ThisTask()
        {
        	return this.task;
        }
        /**
        * 循环执行任务
        * 这也许是线程池的关键�?�?
        */
        public void run() {
            while (isRunning) {
                Task r = null;
                synchronized (taskQueue) {
                    while (taskQueue.isEmpty()) {
                        try {
                            /* 任务队列为空，则等待有新任务加入从�?�被唤醒 */
                            taskQueue.wait(20);
                        } catch (InterruptedException ie) {
                            logger.error(ie);
                        }
                    }
                    /* 取出任务执行 */
                    r = (Task) taskQueue.remove(0);
                }
                if (r != null) {
                    isWaiting = false;
                    try {
//                        if (r.getWriteCommitLog()) {
//                            r.setBeginExceuteTime(new Date());
//                            taskLogger.debug("Worker<" + index
//                                    + "> start execute Task<" + r.getTaskId() + ">");
//                            if (r.getBeginExceuteTime().getTime()
//                                    - r.getSubmitTime().getTime() > 1000)
//                                taskLogger.debug("longer waiting time. "
//                                        + r.info() + ",<" + index + ">,time:"
//                                        + (new Date().getTime() - r
//                                                .getBeginExceuteTime().getTime()));
//                        }
                        this.workername = r.info();
                        task = r;
                        /* 该任务是否需要立即执�? */
                        if (r.needExecuteImmediate()) {
                            new Thread(r).start();
                        } else {
                            r.run();
                        }
//                        if (r.getWriteCommitLog()) {
//                            r.setFinishTime(new Date());
//                            taskLogger.debug("Worker<" + index
//                                    + "> finish task<" + r.getTaskId() + ">");
//                            if (r.getFinishTime().getTime()
//                                    - r.getBeginExceuteTime().getTime() > 1000)
//                                taskLogger.debug("longer execution time. "
//                                        + r.info() + ",<" + index + ">,time:"
//                                        + (r.getFinishTime().getTime() - r
//                                                .getBeginExceuteTime().getTime()));
//                        }
                    } catch (Exception e) {
                        e.printStackTrace();
                        logger.error("##Thread Pool Error:" + this.workername,e);
                    } finally 
                    {
	                    isWaiting = true;
	                    r = null;
                    }
                }
            }
        }
    }
}
