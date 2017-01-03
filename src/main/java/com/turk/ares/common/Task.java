package com.turk.ares.common;

import java.io.Serializable;
import java.util.Date;
import java.util.Date;

import org.apache.log4j.Logger;


/**
 * 
 * @author Administrator
 *
 */
public abstract class Task implements Runnable,Serializable{
    // private static Logger logger = Logger.getLogger(Task.class);
    /*  */
    private Date generateTime = null;
    /* */
    private Date submitTime = null;
    /*  */
    private Date beginExceuteTime = null;
    /*  */
    private Date finishTime = null;
    /**/
    private boolean writecommitlog = true;
    
    /* */
    private String taskname;

    private long taskId;

    public Task() {
        this.generateTime = new Date();
    }

    /**
    * 
    */
    public void run() {
        /**
        * 
        * 
        * beginTransaction();
        * 
        *  subtask = taskCore();
        * 
        * commitTransaction();
        * 
        * ThreadPool.getInstance().batchAddTask(taskCore());
        */
    	try {
			taskCore();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			Logger.getRootLogger().error("Thread Run Error ",e);
		}
    }

    /**
    * 
    * 
    * @throws Exception
    */
    public abstract Task taskCore() throws Exception;

    /**
    *
    * 
    * @return
    */
    protected abstract boolean useDb();

    /**
    *
    * 
    * @return
    */
    protected abstract boolean needExecuteImmediate();
    
    /**
     * stop task
     * @return
     */
    public abstract void stopTask();
    

    /**
    *
    * 
    * @return String
    */
    public abstract String info();

    public Date getGenerateTime() {
        return generateTime;
    }

    public Date getBeginExceuteTime() {
        return beginExceuteTime;
    }

    public void setBeginExceuteTime(Date beginExceuteTime) {
        this.beginExceuteTime = beginExceuteTime;
    }

    public Date getFinishTime() {
        return finishTime;
    }

    public void setFinishTime(Date finishTime) {
        this.finishTime = finishTime;
    }

    public Date getSubmitTime() {
        return submitTime;
    }

    public void setSubmitTime(Date submitTime) {
        this.submitTime = submitTime;
    }

    public long getTaskId() {
        return taskId;
    }

    public void setTaskId(long taskId) {
        this.taskId = taskId;
    }
    
    public void setWriteCommitLog(boolean writecommitlog)
    {
    	this.writecommitlog = writecommitlog;
    }
    
    public boolean getWriteCommitLog()
    {
    	return this.writecommitlog;
    }
}
