package com.turk.ares.common;
import java.util.Collections;
import java.util.Date;
import java.util.LinkedList;
import java.util.List;

import org.apache.log4j.Logger;


/**
 * 
 * @author Administrator
 *
 */
public class ThreadPool {
    private static Logger logger = Logger.getRootLogger();
    private static Logger taskLogger = Logger.getRootLogger();

    private static boolean debug = taskLogger.isDebugEnabled();
    // private static boolean debug = taskLogger.isInfoEnabled();
    /* */
    private static ThreadPool instance = ThreadPool.getInstance();

    public static final int SYSTEM_BUSY_TASK_COUNT = 150;
    /*  */
    public int worker_num = 10;
    
    private String threadName = "Common Thread Pool";
    
    /*  */
    private static int taskCounter = 0;

    public static boolean systemIsBusy = false;

    private List<Task> taskQueue = Collections
            .synchronizedList(new LinkedList<Task>());
    /*  */
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
    * 
    * 
    * @param newTask
    */
    public void addTask(Task newTask) {
        synchronized (taskQueue) {
        	if(taskCounter > 10000000)
        		taskCounter = 0;
            newTask.setTaskId(++taskCounter);
            newTask.setSubmitTime(new Date());
            
            taskQueue.add(newTask);
            /* */
            taskQueue.notifyAll();
        }
        if(newTask.getWriteCommitLog())
	        logger.debug("Submit Task<" + newTask.getTaskId() + ">: "
	                + newTask.info());
    }
    /**
    * 
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
            /*  */
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
    * 
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
     * 
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
    	//
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
    	//
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
     *
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
    * 
    */
    public synchronized void destroy() {
        for (int i = 0; i < workers.length; i++) {
            workers[i].stopWorker();
            workers[i] = null;
        }
        taskQueue.clear();
    }
    
    /**
     * 
     * 
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
      * 
      * 
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
    * 
    * 
    * @author obullxl
    */
    private class PoolWorker extends Thread {
        private int index = -1;
        /*  */
        private boolean isRunning = true;
        /*  */
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
        * 
        * 
        */
        public void run() {
            while (isRunning) {
                Task r = null;
                synchronized (taskQueue) {
                    while (taskQueue.isEmpty()) {
                        try {
                            /* */
                            taskQueue.wait(20);
                        } catch (InterruptedException ie) {
                            logger.error(ie);
                        }
                    }
                    /*  */
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
                        /*  */
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
