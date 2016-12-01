package com.esper.example.model;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import com.espertech.esper.client.EventBean;
import com.espertech.esper.client.EventType;
import com.espertech.esper.core.service.EPRuntimeImpl;
import com.espertech.esper.example.benchmark.InTime;
import com.espertech.esper.example.benchmark.server.StatsHolder;


public class MyThreadPool {
	

    /**  
     * threadFreeSet:空闲线程的ID存放在此
     * 
     */  
    private ConcurrentMap<String,String> threadFreeMap=new ConcurrentHashMap<String,String>();
    
    /**  
     * EXECUTE_TASK_LOCK:执行任务队列的时候，wait()与notiyfAll()的监视对象
     * 当event来临时如果对应type的事件队列是空的，则唤醒一个线程去服务
     *  
     */  
    private static final String EXECUTE_TASK_LOCK=new String("executeTaskLock");
    
    /**
     * coreTypeNum:事件类型数
     */
    private int coreTypeNum;
    
    /**
     * cpuCoreNum:cpu核心数
     */
    private int coreCPUNum = Runtime.getRuntime().availableProcessors();
    
    /**
     * eventTypeService:类型的服务是否在执行
     */
    private ConcurrentMap<EventType,AtomicBoolean> eventTypeServicesMap = new ConcurrentHashMap<EventType,AtomicBoolean>();
    
    /**  
     * workThreadQuery:线程安全的线程队列
     */
    private BlockingQueue<WorkThread> workThreadQuery=new LinkedBlockingQueue<WorkThread>();
    
    /**
     * 模拟任务队列，如果有服务的线程服务该事件，则移出任务队列
     * 在线程完成任务后，将此EventType插入任务队列
     */
    private BlockingQueue<EventType> taskPool = new LinkedBlockingQueue<EventType>();
    
    /**
     * 保存event的资源，其中String作为map的key，value作为保存对应事件类型的BlockQueue
     */
    private ConcurrentHashMap<EventType, BlockingQueue<EventBean>>eventMap = new ConcurrentHashMap<EventType, BlockingQueue<EventBean>>();
    
    
    private EPRuntimeImpl runtime;
    
    /*采用等待策略*/
    public MyThreadPool(EPRuntimeImpl runtime){
    	this.runtime = runtime;
    	EventType[] eventTypeArray = runtime.getEPServicesContext().getEventAdapterService().getAllTypes();  
    	for(EventType type : eventTypeArray){  //初始化所有的数据结构
    		taskPool.add(type);
    		eventMap.put(type,  new LinkedBlockingQueue<EventBean>());
    		eventTypeServicesMap.put(type, new AtomicBoolean(false));
    		
    	}
    	coreTypeNum = eventTypeArray.length;
    	loadWorkThread();
    	startWorkThread();
    	
    }
    
    /*不采用等待策略*/
    public MyThreadPool(EPRuntimeImpl runtime,boolean threadIsFree){
    	this.runtime = runtime;
    	EventType[] eventTypeArray = runtime.getEPServicesContext().getEventAdapterService().getAllTypes();  
    	for(EventType type : eventTypeArray){  //初始化所有的数据结构
    		eventMap.put(type,  new LinkedBlockingQueue<EventBean>());
    		eventTypeServicesMap.put(type, new AtomicBoolean(false));
    	}
    	coreTypeNum = eventTypeArray.length;
    	loadWorkThread();
    	startWorkThread();
    	
    }
    
    /**
     * 加载工作线程
     */
    private void loadWorkThread(){
    	for(int i = 0; i < coreCPUNum; i++){
    		new WorkThread(runtime);
    	}
    }
    
    /**
     * 按照event的类型插入到等待队列中
     * 等待策略
     */
    public void sendEvent(Object theEvent){
    	EventBean eventBean = runtime.wrapEvent(theEvent);
    	
    	try {
			eventMap.get(eventBean.getEventType()).put(eventBean);
			//System.out.println(eventMap.get(eventBean.getEventType()).);
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
    	
    }
    
    /**
     * 不等待策略
     */
    public void sendEvent(Object theEvent,boolean isFree){
    	EventBean eventBean = runtime.wrapEvent(theEvent);
    	EventType eventType = eventBean.getEventType();
//    	boolean isEmpty = eventMap.get(eventBean.getEventType()).isEmpty();
    	eventMap.get(eventType).add(eventBean);
    	if(eventTypeServicesMap.get(eventType).compareAndSet(false, true)){   //队列往taskPool中添加任务
    		taskPool.add(eventType);
    		synchronized(EXECUTE_TASK_LOCK){
                EXECUTE_TASK_LOCK.notifyAll();
            }
    	}
    	
    	
    }
    
    /**
     * 启动所有的工作线程
     */
    public void startWorkThread(){
    	for(WorkThread workThread : workThreadQuery){
    		workThread.start();
    	}
    }
    
    
    
    /*自定义服务线程开始*/
    /**
     * 工作线程去服务每个事件类型的event
     * @author Administrator
     *
     */
    private class WorkThread extends Thread{
    	private String ID;
        private EventType type;
        private boolean isWorking;
        private boolean isDied;
        private EPRuntimeImpl runtime;
        
        public WorkThread(EPRuntimeImpl runtime){
        	this.ID = "" + hashCode();
        	this.runtime = runtime;
        	workThreadQuery.add(this);
        	isDied = false;
        	System.out.println("服务线程"+this.getName() + "加入运行");
            }
        @Override
        public void run() {
            while(!isDied){
            	
            	while(taskPool.isEmpty()&&!isDied){  //开辟的线程过多，导致无需调度的情况下
                    //System.out.println(Thread.currentThread().getName() + "线程,  无任务！睡眠");
                    setStopWorking();
                    try {
                        synchronized(EXECUTE_TASK_LOCK){
                            EXECUTE_TASK_LOCK.wait();
                            //System.out.println(Thread.currentThread().getName() +" 线程被唤醒");
                        }
                    }
                    catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
                
                while(!taskPool.isEmpty()&&!isDied){//非空可以移出一个类型任务去执行事件,可以用poll来获得任务，这样不会在判断的时候被打断
                	EventType eventType = taskPool.poll();
                	if(null == eventType)
                		continue;
                	type = eventType;
                    setStartWorking();
                    eventTypeServicesMap.get(type).set(true);
                    //采用不等待策略进行操作
                    EventBean eventBean = eventMap.get(eventType).poll();
                    if(null != eventBean){
                    	
                    	long ns = System.nanoTime();
                    	InTime inEvent = (InTime)eventBean.getUnderlying();
                    	runtime.processWrappedEvent(eventBean);
                    	taskPool.add(eventType);
                    	long nsDone = System.nanoTime();
                        long msDone = System.currentTimeMillis();
                        StatsHolder.getEngine().update(nsDone - ns);
                        StatsHolder.getServer().update(nsDone - inEvent.getInTime());
                        StatsHolder.getEndToEnd().update(msDone - inEvent.getTime());
                    	
                    }else{
                    	eventTypeServicesMap.get(type).set(false);
                    }
                    
                    //采用不等待策略结束
                    
                    
                    /*//以下的策略是等待策略，利用BlockQueue的特性无资源线程阻塞策略
                    EventBean eventBean;
					try {
						eventBean = eventMap.get(eventType).take();
						runtime.processWrappedEvent(eventBean);
	                    taskPool.add(eventType);
					} catch (InterruptedException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}*/
					//等待策略结束
                    
                    
                    
                }
            }
            System.out.println(Thread.currentThread().getName()+"  死亡");
        }
        public void setStartWorking() {
            this.isWorking = true;
            subtractThreadFreeNum(this.getID());
        }
        public void setStopWorking() {
            this.isWorking = false;
            addThreadFreeNum(this.getID());
        }
        public void setDied(boolean isDied) {
            this.isDied = isDied;
            workThreadQuery.remove(this);
            subtractThreadFreeNum(this.getID());
            synchronized(EXECUTE_TASK_LOCK){
                EXECUTE_TASK_LOCK.notifyAll();
            }
        }
        public EventType getEventType() {
            return type;
        }
        
        public String getID() {
            return ID;
        }
        public void setID(String iD) {
            ID = iD;
        }
        
        
    }
    /*自定义的服务线程结束*/
    
    

    public synchronized void addThreadFreeNum(String id) {
        this.threadFreeMap.put(id,id);
    }
    public synchronized void subtractThreadFreeNum(String id) {
        this.threadFreeMap.remove(id,id);
    }
    

}
