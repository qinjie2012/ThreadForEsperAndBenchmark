package com.esper.linkblocktest;

import com.lmax.disruptor.EventFactory;

/**
 * Created by lw on 14-7-3.
 * <p>
 * 事件对象 - 商品对象
 * <p>
 * 定义 ValueEvent 类，该类作为填充 RingBuffer 的消息，
 * 生产者向该消息中填充数据（就是修改 value 属性值，后文用生产消息代替），
 * 消费者从消息体中获取数据（获取 value 值，后文用消费消息代替）
 *
 * @see com.thread.concurrent_.disruptor.DeliveryReportEventHandler
 */
public final class ValueEvent {

    private Object value;//模拟任务数据

    public Object getValue() {
        return value;
    }

    public void setValue(Object value) {
        this.value = value;
    }

    //定义生成的事件对象，注册到创建 Disruptor 对象
    public final static EventFactory<ValueEvent> EVENT_FACTORY
            = new EventFactory<ValueEvent>() {
        public ValueEvent newInstance() {
            return new ValueEvent();
        }
    };
}