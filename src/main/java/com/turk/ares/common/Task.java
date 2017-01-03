package com.turk.ares.common;

import java.io.Serializable;
import java.util.Date;
import java.util.Date;

import org.apache.log4j.Logger;


/**
 * 线程池任�?
 * @author Administrator
 *
 */
public abstract class Task implements Runnable,Serializable{
    // private static Logger logger = Logger.getLogger(Task.class);
    /* 产生时间 */
    private Date generateTime = null;
    /* 提交执行时间 */
    private Date submitTime = null;
    /* �?始执行时�? */
    private Date beginExceuteTime = null;
    /* 执行完成时间 */
    private Date finishTime = null;
    /*是否写提交日�?*/
    private boolean writecommitlog = true;
    
    /* 任务名称*/
    private String taskname;

    private long taskId;

    public Task() {
        this.generateTime = new Date();
    }

    /**
    * 任务执行入口
    */
    public void run() {
        /**
        * 相关执行代码
        * 
        * beginTransaction();
        * 
        * 执行过程中可能产生新的任�? subtask = taskCore();
        * 
        * commitTransaction();
        * 
        * 增加新产生的任务 ThreadPool.getInstance().batchAddTask(taskCore());
        */
    	try {
			taskCore();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			Logger.getRootLogger().error("Thread Run Error ",e);
		}
    }

    /**
    * �?有任务的核心 �?以特别的业务逻辑执行之处
    * 
    * @throws Exception
    */
    public abstract Task taskCore() throws Exception;

    /**
    * 是否用到数据�?
    * 
    * @return
    */
    protected abstract boolean useDb();

    /**
    * 是否�?要立即执�?
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
    * 任务信息
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
