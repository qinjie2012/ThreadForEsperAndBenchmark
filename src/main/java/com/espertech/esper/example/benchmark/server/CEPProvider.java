/**************************************************************************************
 * Copyright (C) 2007 Esper Team. All rights reserved.                                *
 * http://esper.codehaus.org                                                          *
 * ---------------------------------------------------------------------------------- *
 * The software in this package is published under the terms of the GPL license       *
 * a copy of which has been included with this distribution in the license.txt file.  *
 **************************************************************************************/
package com.espertech.esper.example.benchmark.server;

import com.esper.example.model.MyThreadPool;
import com.espertech.esper.client.Configuration;
import com.espertech.esper.client.EPAdministrator;
import com.espertech.esper.client.EPRuntime;
import com.espertech.esper.client.EPServiceProvider;
import com.espertech.esper.client.EPServiceProviderManager;
import com.espertech.esper.client.EPStatement;
import com.espertech.esper.client.EventBean;
import com.espertech.esper.client.UpdateListener;
import com.espertech.esper.core.service.EPRuntimeImpl;
import com.espertech.esper.example.benchmark.MarketData;
import com.espertech.esper.example.benchmark.NewsData;
import com.espertech.esper.example.benchmark.UsersData;

/**
 * A factory and interface to wrap ESP/CEP engine dependency in a single space
 *
 * @author Alexandre Vasseur http://avasseur.blogspot.com
 */
public class CEPProvider {

    public static interface ICEPProvider {//�����ڲ�����һ���ӿڣ������ҲΪ������  

        public void init(int sleepListenerMillis);
        
        public void initPool();

        public void registerStatement(String statement, String statementID);

        public void sendEvent(Object theEvent);
        
        public void sendEventByPool(Object theEvent);
    }

    public static ICEPProvider getCEPProvider() {
    	String className = System.getProperty("esper.benchmark.provider", EsperCEPProvider.class.getName());
       // Properties p = System.getProperties();
       // p.list(System.out);
        try {
            Class klass = Class.forName(className);
            return (ICEPProvider) klass.newInstance();
        } catch (Throwable t) {
            t.printStackTrace();
            throw new RuntimeException(t);
        }
    }

    public static class EsperCEPProvider implements ICEPProvider {

        private EPAdministrator epAdministrator;

        private EPRuntime epRuntime;
        
        private MyThreadPool pool;

        public EPRuntime getEpRuntime() {
			return epRuntime;
		}

		public void setEpRuntime(EPRuntime epRuntime) {
			this.epRuntime = epRuntime;
		}

		// only one of those 2 will be attached to statement depending on the -mode selected
        private UpdateListener updateListener;
        private MySubscriber subscriber;
        private MySubscriber1 subscriber1;

        private static int sleepListenerMillis;

        public EsperCEPProvider() {
        }

        public void init(final int _sleepListenerMillis) {
            sleepListenerMillis = _sleepListenerMillis;
            Configuration configuration;

            // EsperHA enablement - if available
            try {//Class.forName�ǽ�����װ�ص�JVM��
                Class configurationHAClass = Class.forName("com.espertech.esperha.client.ConfigurationHA");
                configuration = (Configuration) configurationHAClass.newInstance();//�൱��Configuration configuration=new Configuration;
                System.out.println("=== EsperHA is available, using ConfigurationHA ===");
            } catch (ClassNotFoundException e) {
                configuration = new Configuration();
            } catch (Throwable t) {
                System.err.println("Could not properly determine if EsperHA is available, default to Esper");
                t.printStackTrace();
                configuration = new Configuration();
            }
            configuration.addEventType("Market", MarketData.class);
            configuration.addEventType("News", NewsData.class);
            configuration.addEventType("Users", UsersData.class);


            // EsperJMX enablement - if available
			try {
				Class.forName("com.espertech.esper.jmx.client.EsperJMXPlugin");
	            configuration.addPluginLoader(
	                    "EsperJMX",
	                    "com.espertech.esper.jmx.client.EsperJMXPlugin",
	    				null);// will use platform mbean - should enable platform mbean connector in startup command line
                System.out.println("=== EsperJMX is available, using platform mbean ===");
			} catch (ClassNotFoundException e) {
				;
			}


            EPServiceProvider epService = EPServiceProviderManager.getProvider("benchmark", configuration);
            epAdministrator = epService.getEPAdministrator();
            updateListener = new MyUpdateListener();
            subscriber = new MySubscriber();
            subscriber1 =new MySubscriber1();
            epRuntime = epService.getEPRuntime();
            
        }

        public void registerStatement(String statement, String statementID) {
            EPStatement stmt = epAdministrator.createEPL(statement, statementID);
         //   System.out.println(System.getProperty("esper.benchmark.ul"));
            if (System.getProperty("esper.benchmark.ul") != null) {
                stmt.addListener(updateListener);
            } else {
            //	System.out.println("ִ��stmt.setSubscriber(subscriber)����");
           /* 	System.out.println(statementID);*/
                stmt.setSubscriber(subscriber);
          /*      if(statementID=="S1")
                stmt.setSubscriber(subscriber1);*/
            }
        }
        
        public void initPool(){
        	pool = new MyThreadPool((EPRuntimeImpl)epRuntime, true);
        }
        
        public void sendEventByPool(Object theEvent) {
        	pool.sendEvent(theEvent,true);
        }
        
        public void sendEvent(Object theEvent) {
            epRuntime.sendEvent(theEvent);
        }
    }

    public static class MyUpdateListener implements UpdateListener {
        public void update(EventBean[] newEvents, EventBean[] oldEvents) {
            if (newEvents != null) {
            	int price = (Integer) newEvents[0].get("price");
            	System.out.println("The Price is :"+price);
                if (EsperCEPProvider.sleepListenerMillis > 0) {
                    try {
                        Thread.sleep(EsperCEPProvider.sleepListenerMillis);
                    } catch (InterruptedException ie) {
                        ;
                    }
                }
            }
        }
    }
    
    public static class MySubscriber1{
    	public void update(MarketData marketData) {
        	System.out.println("S999��ѯ----��Ʊ����: "+marketData.getTicker()+"    �۸���: "+marketData.getPrice()+"     �ɽ�����: "+marketData.getVolume()+"     ������: "+marketData.getCountry());
            if (EsperCEPProvider.sleepListenerMillis > 0) {
                try {
                    Thread.sleep(EsperCEPProvider.sleepListenerMillis);
                } catch (InterruptedException ie) {
                    ;
                }
            }
        }
    }

    public static class MySubscriber {
    	
    	public void update(MarketData marketData,NewsData newsData) {
    		//System.out.println("��Ʊ����: "+marketData.getTicker()+"    �۸���: "+marketData.getPrice()+"     �ɽ�����: "+marketData.getVolume()+"     ������: "+marketData.getCountry());
        	//System.out.println("���ű���ǣ�"+newsData.getNewsId()+"     ��Ʊ����: "+newsData.getTicker()+"    ������: "+newsData.getDate()+"     ������֯��: "+newsData.getOrganization());
    		/*System.out.println("��Ʊ����: "+marketData.getTicker()+"    �۸���: "+marketData.getPrice()+"     �ɽ�����: "+marketData.getVolume()+"     ������: "+marketData.getCountry()
    				+"       ���ű���ǣ�"+newsData.getNewsId()+"    ����������: "+newsData.getDate()+"     ������֯��: "+newsData.getOrganisation());
    		*/
    		if (EsperCEPProvider.sleepListenerMillis > 0) {
                try {
                    Thread.sleep(EsperCEPProvider.sleepListenerMillis);
                } catch (InterruptedException ie) {
                    ;
                }
            }
        }
    	
    	//�����¼��������Ӳ���
    	public void update(MarketData marketData,NewsData newsData,UsersData usersData) {
    		/*System.out.println("��Ʊ����: "+marketData.getTicker()+"    �۸���: "+marketData.getPrice()+"     �ɽ�����: "+marketData.getVolume()+"     ������: "+marketData.getCountry()
    				+"       ���ű���ǣ�"+newsData.getNewsId()+"    ����������: "+newsData.getDate()+"     ������֯��: "+newsData.getOrganisation()
    				+"       �û�����ǣ�"+usersData.getUsersId()+"      �û���Ʊ�������ǣ�"+usersData.getHoldvolume());
    		*/
    		if (EsperCEPProvider.sleepListenerMillis > 0) {
                try {
                    Thread.sleep(EsperCEPProvider.sleepListenerMillis);
                } catch (InterruptedException ie) {
                    ;
                }
            }
        }
    	
        public void update(NewsData newsData) {
        	//System.out.println("���ű���ǣ�"+newsData.getNewsId()+"     ��Ʊ����: "+newsData.getTicker()+"    ����������: "+newsData.getDate()+"       ������֯��: "+newsData.getOrganisation());
            if (EsperCEPProvider.sleepListenerMillis > 0) {
                try {
                    Thread.sleep(EsperCEPProvider.sleepListenerMillis);
                } catch (InterruptedException ie) {
                    ;
                }
            }
        }
        
        public void update(UsersData usersData) {
        	//System.out.println("�û�����ǣ�"+usersData.getUsersId()+"    ���ű���ǣ�"+usersData.getNewsId()+"     ��Ʊ����: "+usersData.getTicker()+"    ��Ʊ��������: "+usersData.getHoldvolume());
            if (EsperCEPProvider.sleepListenerMillis > 0) {
                try {
                    Thread.sleep(EsperCEPProvider.sleepListenerMillis);
                } catch (InterruptedException ie) {
                    ;
                }
            }
        }
    	
    	//��Ӧ��S1��S2��S5��ѯ��update����
        public void update(MarketData marketData) {
            	//System.out.println("��Ʊ����: "+marketData.getTicker()+"    �۸���: "+marketData.getPrice()+"     �ɽ�����: "+marketData.getVolume()+"     ������: "+marketData.getCountry());
                if (EsperCEPProvider.sleepListenerMillis > 0) {
                    try {
                        Thread.sleep(EsperCEPProvider.sleepListenerMillis);
                    } catch (InterruptedException ie) {
                        ;
                    }
                }
            }
        
        
        //��Ӧ��S3��ѯ��update������
    	public void update(MarketData marketData,double turnover) {
    		//System.out.println("��Ʊ����: "+marketData.getTicker()+"    �۸���: "+marketData.getPrice()+"     �ɽ�����: "+marketData.getVolume()+"     ������: "+marketData.getCountry()+"    �ܳɽ����ǣ�"+turnover );
            if (EsperCEPProvider.sleepListenerMillis > 0) {
                try {
                    Thread.sleep(EsperCEPProvider.sleepListenerMillis);
                } catch (InterruptedException ie) {
                    ;
                }
            }
        }
    	
    	
      	//��Ӧ��S4��ѯ��update������
      /*  public void update(double turnover) {
        	//if(mode==)
        	System.out.println("��Ʊ������Ϊ: "+turnover);
            if (EsperCEPProvider.sleepListenerMillis > 0) {
                try {
                    Thread.sleep(EsperCEPProvider.sleepListenerMillis);
                } catch (InterruptedException ie) {
                    ;
                }
            }
        }*/
    	
    	
    	//��ѯ���A1
    	public void update(String ticker, double avg_price, String country) {
        	//System.out.println("��Ʊ��Ϊ��"+ticker+"     ��Ʊƽ�����׼۸�Ϊ: "+avg_price+"     ����Ϊ��"+country);
            if (EsperCEPProvider.sleepListenerMillis > 0) {
                try {
                    Thread.sleep(EsperCEPProvider.sleepListenerMillis);
                } catch (InterruptedException ie) {
                    ;
                }
            }
        }
    	
    	//A2
    	public void update(long count, String ticker) {
        	//System.out.println("��Ʊ��Ϊ��"+ticker+"     ��Ʊƽ�����׼۸�Ϊ: "+avg_price+"     ����Ϊ��"+country);
            if (EsperCEPProvider.sleepListenerMillis > 0) {
                try {
                    Thread.sleep(EsperCEPProvider.sleepListenerMillis);
                } catch (InterruptedException ie) {
                    ;
                }
            }
        }
    	
    	
    	//��ѯ���A2
    	/*public void update(String ticker, double max_price) {
        	System.out.println("��Ʊ��Ϊ�� "+ticker+"     ��Ʊ���׼۸����ֵΪ�� "+max_price);
            if (EsperCEPProvider.sleepListenerMillis > 0) {
                try {
                    Thread.sleep(EsperCEPProvider.sleepListenerMillis);
                } catch (InterruptedException ie) {
                    ;
                }
            }
        }*/
    	
    	
    	//��ѯ���A3
    	public void update(double sum_price) {
        	//System.out.println("��Ʊ�����ܼ۸�Ϊ********: "+sum_price);
            if (EsperCEPProvider.sleepListenerMillis > 0) {
                try {
                    Thread.sleep(EsperCEPProvider.sleepListenerMillis);
                } catch (InterruptedException ie) {
                    ;
                }
            }
        }
    	
    	
    	
    	
    	//��ѯ���A5
    	/*public void update(String ticker, double median_price) {
        	System.out.println("��Ʊ��Ϊ�� "+ticker+"     ��Ʊ���׼۸��м�ֵΪ�� "+median_price);
            if (EsperCEPProvider.sleepListenerMillis > 0) {
                try {
                    Thread.sleep(EsperCEPProvider.sleepListenerMillis);
                } catch (InterruptedException ie) {
                    ;
                }
            }
        }*/
    	
    	
    	//��ѯ���A7
    	public void update(String ticker, double average) {
        	//System.out.println("��Ʊ��Ϊ�� "+ticker+"     ��Ʊ���׼�Ȩƽ��ֵΪ�� "+average);
            if (EsperCEPProvider.sleepListenerMillis > 0) {
                try {
                    Thread.sleep(EsperCEPProvider.sleepListenerMillis);
                } catch (InterruptedException ie) {
                    ;
                }
            }
        }
    	
    	
    	//��ѯ���A5
    	public void update(double avg_price, String country) {
        	//System.out.println("��Ʊƽ��ֵΪ�� "+avg_price+"     ��Ʊ����Ϊ�� "+country);
            if (EsperCEPProvider.sleepListenerMillis > 0) {
                try {
                    Thread.sleep(EsperCEPProvider.sleepListenerMillis);
                } catch (InterruptedException ie) {
                    ;
                }
            }
        }
    	
    	
    	
    	
    	//��ѯ���A6
    	public void update(long count) {
        	//System.out.println("��������¼���Ϊ: "+count);
            if (EsperCEPProvider.sleepListenerMillis > 0) {
                try {
                    Thread.sleep(EsperCEPProvider.sleepListenerMillis);
                } catch (InterruptedException ie) {
                    ;
                }
            }
        }
        
        
      	//��Ӧ��S6��ѯ��update������
       /* public void update(Map<String,String> map) {
        	Iterator<String> iter = map.keySet().iterator();
        	while(iter.hasNext()){
        		String key = iter.next();
        		System.out.println("�ѵ�û��ֵô��");
        		System.out.println("key:"+key+"value:"+map.get(key));
	    		if (EsperCEPProvider.sleepListenerMillis > 0) {
	                try {
	                	Thread.sleep(EsperCEPProvider.sleepListenerMillis);
	                	} catch (InterruptedException ie) {
	                		;
	                		}
	                }
	    		}
        }
    	*/
    	
    	//��ѯ���A7
    	public void update(MarketData first, MarketData last) {
    		/*System.out.println("**************************************************************");
    		System.out.println("���������¼���Ʊ����: "+first.getTicker()+"    �۸���: "+first.getPrice()+"     �ɽ�����: "+first.getVolume());
    		System.out.println("��������¼���Ʊ����: "+last.getTicker()+"    �۸���: "+last.getPrice()+"     �ɽ�����: "+last.getVolume());
    		System.out.println("**************************************************************");
            */
            if (EsperCEPProvider.sleepListenerMillis > 0) {
                try {
                    Thread.sleep(EsperCEPProvider.sleepListenerMillis);
                } catch (InterruptedException ie) {
                    ;
                }
            }
        }
    	
  


    
        
        public void update(String ticker) {
        	//System.out.println("��Ʊ��Ϊ: "+ticker);
            if (EsperCEPProvider.sleepListenerMillis > 0) {
                try {
                    Thread.sleep(EsperCEPProvider.sleepListenerMillis);
                } catch (InterruptedException ie) {
                    ;
                }
            }
        }


        public void update(String ticker, double avg, long count, double sum) {
        	//System.out.println("ticker: "+ticker+"   avgPrice is: "+avg+"   count is: "+count+"   sum is: "+sum);
            if (EsperCEPProvider.sleepListenerMillis > 0) {
                try {
                    Thread.sleep(EsperCEPProvider.sleepListenerMillis);
                } catch (InterruptedException ie) {
                    ;
                }
            }
        }
    }

}
