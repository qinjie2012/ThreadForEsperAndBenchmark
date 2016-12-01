package com.esper.linkblocktest;

import java.util.concurrent.locks.Lock;

import com.espertech.esper.core.service.EPRuntimeImpl;
import com.lmax.disruptor.EventHandler;

public class EsperEventHandle implements EventHandler<ValueEvent> {

	private int id;// 消费者编号
	private final EPRuntimeImpl runtime;   //必须传递过来
	
	private Lock lock;  //如果没有获得这个lock结构，则要阻塞不同的消费者的后续操作

	public EsperEventHandle(int id, EPRuntimeImpl runtime) {
		this.id = id;
		this.runtime = runtime;
	}

	public int getId() {
		return id;
	}

	public void setId(int id) {
		this.id = id;
	}
	



	public EPRuntimeImpl getRuntime() {
		return runtime;
	}



	@Override
	public String toString() {
		return "EsperEventHandle{" + "id=" + id + '}';
	}

	/**
	 * @param event
	 *            事件
	 * @param sequence
	 *            事件正在处理
	 * @param endOfBatch
	 *            是否是最后一个事件在处理
	 * @throws Exception
	 *             Exception
	 */
	@Override
	public void onEvent(ValueEvent event, long sequence, boolean endOfBatch)
			throws Exception {
		runtime.processEvent(event.getValue());   //传递Event

	}
}
