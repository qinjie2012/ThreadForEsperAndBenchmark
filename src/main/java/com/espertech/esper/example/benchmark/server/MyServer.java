/**************************************************************************************
 * Copyright (C) 2007 Esper Team. All rights reserved.                                *
 * http://esper.codehaus.org                                                          *
 * ---------------------------------------------------------------------------------- *
 * The software in this package is published under the terms of the GPL license       *
 * a copy of which has been included with this distribution in the license.txt file.  *
 **************************************************************************************/
package com.espertech.esper.example.benchmark.server;

import com.espertech.esper.example.benchmark.Symbols;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintStream;
import java.net.InetSocketAddress;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.Enumeration;
import java.util.Properties;
import java.util.Scanner;
import java.util.Set;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.*;

/**
 * The main Esper Server thread listens on the given port.
 * It bootstrap an ESP/CEP engine (defaults to Esper) and registers EPL statement(s) into it based
 * on the given -mode argument.
 * Statements are read from an statements.properties file in the classpath
 * If statements contains '$' the '$' is replaced by a symbol string, so as to register one statement per symbol.
 * <p/>
 * Based on -queue, the server implements a direct handoff to the ESP/CEP engine, or uses a Syncrhonous queue
 * (somewhat an indirect direct handoff), or uses a FIFO queue where each events is put/take one by one from the queue.
 * Usually with few clients sending a lot of events, use the direct handoff, else consider using queues. Consumer thread
 * can be configured using -thread (it will range up to #processor x #thread).
 * When queues is full, overload policy triggers execution on the caller side.
 * <p/>
 * To simulate an ESP/CEP listener work, use -sleep.
 * <p/>
 * Use -stat to control how often percentile stats are displayed. At each display stats are reset.
 * <p/>
 * If you use -rate nxM (n threads, M event/s), the server will simulate the load for a standalone simulation without
 * any remote client(s).
 * <p/>
 * By default the benchmark registers a subscriber to the statement(s). Use -Desper.benchmark.ul to use
 * an UpdateListener instead. Note that the subscriber contains suitable update(..) methods for the default
 * proposed statement in the statements.properties files but might not be suitable if you change statements due
 * to the strong binding with statement results. 
 *
 * @author Alexandre Vasseur http://avasseur.blogspot.com
 */
public class MyServer extends Thread {

    private int port;
    private int threadCore;
    private int queueMax;
    private int sleepListenerMillis;
    private int statSec;
    private int simulationRate;
    private int simulationThread;
    private String mode;

    public static final int DEFAULT_PORT = 1234;
    public static final int DEFAULT_THREADCORE = Runtime.getRuntime().availableProcessors();
    public static final int DEFAULT_QUEUEMAX = -1;
    public static final int DEFAULT_SLEEP = 0;
    public static final int DEFAULT_SIMULATION_RATE = -1;//-1: no simulation
    public static final int DEFAULT_SIMULATION_THREAD = -1;//-1: no simulation
    public static final int DEFAULT_STAT = 1;//���ڿ���ͳ�ƽ������Ƶ��
    public static final String DEFAULT_MODE = "S1";
    public static final Properties MODES = new Properties();


    private CEPProvider.ICEPProvider cepProvider;

    public MyServer(String mode, int port, int threads, int queueMax, int sleep, final int statSec, int simulationThread, final int simulationRate) {
        super("EsperServer-main");
        this.mode = mode;
        this.port = port;
        this.threadCore = threads;
        this.queueMax = queueMax;
        this.sleepListenerMillis = sleep;
        this.statSec = statSec;
        this.simulationThread = simulationThread;
        this.simulationRate = simulationRate;

        // turn on stat dump
        Timer t = new Timer("EsperServer-stats", true);
       
       
        t.scheduleAtFixedRate(new TimerTask() {
            public void run() {	
                StatsHolder.dump("engine");
               // StatsHolder.dump("server");
                //StatsHolder.dump("endToEnd");
                StatsHolder.reset();
                if (simulationRate <= 0) {
                	ClientConnectionOfMy.dumpStats(statSec);
                } else {
                    SimulateClientConnection.dumpStats(statSec);
                }
            }
        }, 0L, statSec * 1000);
    }

    public void setCEPProvider(CEPProvider.ICEPProvider cepProvider) {
        this.cepProvider = cepProvider;
    }

    public synchronized void start() {
        // register ESP/CEP engine
        cepProvider = CEPProvider.getCEPProvider();
        cepProvider.init(sleepListenerMillis);
        
        
        File file = new File("Test3.txt");
		BufferedReader reader = null;
		String tempString = null;
		String s1[]=new String[3];//���ڴ洢��ѯ���ı�źͲ�ѯ��䱾���Լ��ò�ѯ��������
		int count,j;//���ڼ�¼ÿ����ѯ���ע��Ĵ���
		try{
			
			reader = new BufferedReader(new FileReader(file));	
			while((tempString = reader.readLine())!=null){
				//System.out.println(tempString);
				s1=tempString.split("#");
				count=Integer.parseInt(s1[2]);
				System.out.println(count);
				for(j=0;j<count;j++){
					if(s1[1].indexOf('$')<0){
						System.out.println(s1[1]);
						cepProvider.registerStatement(s1[1], s1[0]);
					}
					else{
						String ticker = Symbols.SYMBOLS[j];						
						System.out.println(s1[1].replaceAll("\\$", ticker));
						cepProvider.registerStatement(s1[1].replaceAll("\\$", ticker), s1[0]);
					}
				}
			}
			reader.close();
		}catch(IOException e){
			e.printStackTrace();
		}

        

        super.start();
    }

    public void run() {
        if (simulationRate <= 0) {
            runServer();
        } else {
            System.out.println("运行结束!!!");
        }
    }

    public void runServer() {
        try {
            System.out.println((new StringBuilder("Server accepting connections on port ")).append(port).append(".").toString());
            ServerSocketChannel serverSocketChannel = ServerSocketChannel.open();
            //Java NIO�е� ServerSocketChannel ��һ�����Լ����½�����TCP���ӵ�ͨ��, �����׼IO�е�ServerSocketһ��
            serverSocketChannel.socket().bind(new InetSocketAddress(port));
            //��ServerSocket���ض���ַ
            do {
                SocketChannel socketChannel = serverSocketChannel.accept();
                //ͨ�� ServerSocketChannel.accept() ���������½���������
                System.out.println("Client connected to server.");
                ClientConnectionOfMy a=new ClientConnectionOfMy(socketChannel, cepProvider, statSec);
                a.start();//ClientConnection�̳���Thread������start������
                //(new ClientConnection(socketChannel, executor, cepProvider, statSec)).start();
            } while (true);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    

    public static void main(String argv[]) throws IOException {
        // load modes
        //MODES.load(Server.class.getClassLoader().getResourceAsStream("statements.properties"));
        MODES.put("NOOP", "");

        int port = DEFAULT_PORT;
        int threadCore = DEFAULT_THREADCORE;
        int queueMax = DEFAULT_QUEUEMAX;
        int sleep = DEFAULT_SLEEP;
        int simulationRate = DEFAULT_SIMULATION_RATE;
        int simulationThread = DEFAULT_SIMULATION_THREAD;
        String mode = DEFAULT_MODE;
        int stats = DEFAULT_STAT;
        for (int i = 0; i < argv.length; i++)
            if ("-port".equals(argv[i])) {
                i++;
                port = Integer.parseInt(argv[i]);
            } else if ("-thread".equals(argv[i])) {
                i++;
                threadCore = Integer.parseInt(argv[i]);
            } else if ("-queue".equals(argv[i])) {
                i++;
                queueMax = Integer.parseInt(argv[i]);
            } else if ("-sleep".equals(argv[i])) {
                i++;
                sleep = Integer.parseInt(argv[i]);
            } else if ("-stat".equals(argv[i])) {
                i++;
                stats = Integer.parseInt(argv[i]);
            } else if ("-mode".equals(argv[i])) {
                i++;
                mode = argv[i];
                if (MODES.getProperty(mode) == null) {
                    System.err.println("Unknown mode");
                    printUsage();
                }
            } else if ("-rate".equals(argv[i])) {
                i++;
                int xIndex = argv[i].indexOf('x');
                simulationThread = Integer.parseInt(argv[i].substring(0, xIndex));
                simulationRate = Integer.parseInt(argv[i].substring(xIndex + 1));
            } else {
                printUsage();
            }
   /*     System.out.println("Please input the value of mode: ");
        Scanner s=new Scanner(System.in);
        mode=s.next();*/
        MyServer bs = new MyServer(mode, port, threadCore, queueMax, sleep, stats, simulationThread, simulationRate);
        bs.start();
        System.out.println(threadCore);
        System.out.println(Runtime.getRuntime().availableProcessors() * threadCore);
        try {
            bs.join();//�����̣߳������̵߳ȴ����̵߳���ֹ��Ҳ���������̵߳�����join()��������Ĵ��룬ֻ�еȵ����߳̽����˲���ִ�С�
        } catch (InterruptedException e) {
            ;
        }
    }

    private static void printUsage() {
        System.err.println("usage: com.espertech.esper.example.benchmark.server.Server <-port #> <-thread #> <-queue #> <-sleep #> <-stat #> <-rate #x#> <-mode xyz>");
        System.err.println("defaults:");
        System.err.println("  -port:    " + DEFAULT_PORT);
        System.err.println("  -thread:  " + DEFAULT_THREADCORE);
        System.err.println("  -queue:   " + DEFAULT_QUEUEMAX + "(-1: no executor, 0: SynchronousQueue, n: LinkedBlockingQueue");
        System.err.println("  -sleep:   " + DEFAULT_SLEEP + "(no sleep)");
        System.err.println("  -stat:   " + DEFAULT_STAT + "(s)");
        System.err.println("  -rate:    " + DEFAULT_SIMULATION_RATE + "(no standalone simulation, else <n>x<evt/s> such as 2x1000)");
        System.err.println("  -mode:    " + "(default " + DEFAULT_MODE + ", choose from " + MODES.keySet().toString() + ")");
        System.err.println("Modes are read from statements.properties in the classpath");
        System.exit(1);
    }

}
