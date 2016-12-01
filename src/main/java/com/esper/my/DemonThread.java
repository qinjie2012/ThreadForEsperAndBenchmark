package com.esper.my;

import java.util.List;
import java.util.Map;

import com.esper.linkblocktest.Esper_Disruptor;
import com.espertech.esper.client.EventBean;
import com.espertech.esper.client.EventType;
import com.espertech.esper.core.service.EPRuntimeImpl;
import com.espertech.esper.core.service.EPServicesContext;

public class DemonThread {
	private EPServicesContext services;
	private int eventStreamNum; // 事件流个数
	private List[] buffStream; // 作为每个的缓冲
	private Map<EventType,Integer> map;
	private EPRuntimeImpl runtime; // 需要esper核心参数
	private List<Esper_Disruptor> eventTypeDisruptor;
	public DemonThread(final EPServicesContext services,EPRuntimeImpl runtime) { // 传递service是为了查询此结构中找到多少个事件流
		super();
		this.services = services;
		this.runtime = runtime;
		// eventStreamNum =
		// services.getStatementEventTypeRefService().getStatementNamesForType(eventTypeName);//找到流的个数
	}

	/*
	 * public void run(){ //维护缓冲区，并且维护一个线程池去分配
	 * 
	 * }
	 */

	public void sendEvent(Object theEvent) {//添加到缓冲区
		EventBean eventBean;
		eventBean = runtime.wrapEvent(theEvent);
		EventType type = eventBean.getEventType();
		int loc = map.get(type);
		switch(loc){   //根据找到的位置来判断选用对应的生产者消费者对
			
		}
	}
	
	private void processEvent(){
		
	}

}
