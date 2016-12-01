package com.esper.example;


import com.esper.example.model.Product;
import com.esper.example.model.Product1;
import com.espertech.esper.client.Configuration;
import com.espertech.esper.client.EPAdministrator;
import com.espertech.esper.client.EPRuntime;
import com.espertech.esper.client.EPServiceProvider;
import com.espertech.esper.client.EPServiceProviderManager;
import com.espertech.esper.client.EPStatement;
import com.espertech.esper.client.EventBean;
import com.espertech.esper.client.UpdateListener;

/**
 * 
 * @datetime : 2015年10月14日 下午1:02:58 by Administrator
 *
 * @version : 1.0.0
 *
 * 描述: avg函数用于计算view里包含的事件的平均值，具体计算的平均值是由avg里的内容决定的
 * 		以如下view所示，如果为length，则每次事件进入并触发监听器时，只计算view里的平均price，不会将oldEvent计算在内 
 * 		如果为length_batch，则请参看AverageBatchTest例子
 *
 */
public class AverageTest {

	public static void main(String[] args) throws InterruptedException {
		Configuration configuration = new Configuration();
		//configuration.getEngineDefaults().getMetricsReporting().setEnableMetricsReporting(true);
		configuration.getEngineDefaults().getThreading().setThreadPoolInbound(true);
		configuration.getEngineDefaults().getThreading().setThreadPoolInboundNumThreads(3);
		//configuration.getEngineDefaults().getLogging().setEnableQueryPlan(true);
		EPServiceProvider epService = EPServiceProviderManager.getProvider("ThreadPoolInbound3", configuration);
		
		//EPServiceProvider epService = EPServiceProviderManager.getDefaultProvider();
		
		EPAdministrator admin = epService.getEPAdministrator();
		
		String product = Product.class.getName();
		String product1 = Product1.class.getName();
		String epl1 = "select istream  sum(price) from " + product + ".win:length(3) where " +   " price > 1";
		String epl2 = "select istream  price from " + product1 + ".win:length(2) ";
		
		
		
		/*String epl3 = "select istream  sum(price) from " + product1 + ".win:time(4 sec) ";
		String epl4 = "select istream avg(P1.price) as avgP1 from " + product  + ".win:length(3) as P1," 
					   + product1 + ".win:length(2) as P2," + product  + ".win:length(4) as P3 " + " where P1.price = P2.price";
		String epl5 = "select istream  sum(price) from " + product2 + ".win:time(4 sec) ";*/
		
		String epl3 = "select rstream  sum(price) from " + product1 + ".win:length(2) order by price";
		
		String epl4 = "select istream avg(P1.price) as avgP1 from " + product  + ".win:length(2) as P1," + product  + ".win:time(2) as P3," + product1 + ".win:length(2) as P2" + " where P1.price = P2.price";
		String epl5 = "select istream avg(P1.price) as avgP1 from " + product  + ".win:length(2) as P1," + product1 + ".win:length(2) as P2" + " where P1.price = P2.price";
		
		//String eplMetric = "select * from com.espertech.esper.client.metric.StatementMetric";
		EPStatement state1 =  admin.createEPL(epl1,"statement01");
		EPStatement state2 =  admin.createEPL(epl2,"statement02"); //对应事件Product
		EPStatement state4 =  admin.createEPL(epl4,"statement04");
		EPStatement state5 =  admin.createEPL(epl5,"statement05");
		//EPStatement state5 =  admin.createEPL(epl5,"statement05");
		/*admin.createEPL(epl2,"statement02"); //重复创建EPL2
		EPStatement state3 =  admin.createEPL(epl3,"statement03");// 对应事件Product1    设计带有join的查询   跟踪查询的结果
		EPStatement state4 = admin.createEPL(epl4);
		admin.createEPL(eplMetric).addListener(new MetricListener());;*/
		//state1.addListener(new AverageListenerEpl1());  //注册Epl1的监听器
		state1.addListener(new AverageListenerEpl1());
		state4.addListener(new AverageListenerEpl4());  //注册Epl4语句的监听器
		
		EPRuntime runtime = epService.getEPRuntime();

		
		
		Product product_1 = new Product();    //product事件
		product_1.setPrice(1);
		product_1.setType("product_1");
		//esper_disruptor.sendEsperEvent(product_1);
		runtime.sendEvent(product_1);
		
		Product product_2 = new Product();    //product事件
		product_2.setPrice(2);
		product_2.setType("product_2");
		//esper_disruptor.sendEsperEvent(product_2);
		runtime.sendEvent(product_2);
		
		Product1 product1_0 = new Product1();  //product1事件，触发join连接查询   epl4
		product1_0.setPrice(1);
		product1_0.setType("product1_0");
		//esper_disruptor.sendEsperEvent(product1_0);
		runtime.sendEvent(product1_0);
		
		Product1 product1_1 = new Product1();  //product1事件，触发join连接查询   epl4
		product1_1.setPrice(2);
		product1_1.setType("product1_1");
		runtime.sendEvent(product1_1);

		Product product_3 = new Product();     //product事件
		product_3.setPrice(3);
		product_3.setType("product_3");
		//esper_disruptor.sendEsperEvent(product_3);
		runtime.sendEvent(product_3);

		Product product_4 = new Product();     //product事件
		product_4.setPrice(4);
		product_4.setType("product_4");
		runtime.sendEvent(product_4);
	}
}

class AverageListenerEpl4 implements UpdateListener {

	public void update(EventBean[] newEvents, EventBean[] oldEvents) {
		if (newEvents != null) {
			EventBean event = newEvents[0];
			System.out.println("Epl4->avg(P1.price): " + event.get("avgP1") + Thread.currentThread().getName());
		}
	}
	
	
}



class AverageListenerEpl1 implements UpdateListener {

	public void update(EventBean[] newEvents, EventBean[] oldEvents) {
		if (newEvents != null) {
			EventBean event = newEvents[0];
			System.out.println("Epl1->sum price(): " + event.get("sum(price)") + Thread.currentThread().getName());
		}
	}
}

class MetricListener implements UpdateListener {

	public void update(EventBean[] newEvents, EventBean[] oldEvents) {
		if (newEvents != null) {
			EventBean event = newEvents[0];
			System.out.println("statementName: " + event.get("statementName") + ", cpuTime: " + event.get("cpuTime"));
		}
	}
}
