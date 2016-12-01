/**************************************************************************************
 * Copyright (C) 2007 Esper Team. All rights reserved.                                *
 * http://esper.codehaus.org                                                          *
 * ---------------------------------------------------------------------------------- *
 * The software in this package is published under the terms of the GPL license       *
 * a copy of which has been included with this distribution in the license.txt file.  *
 **************************************************************************************/
package com.espertech.esper.example.benchmark.server;

import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ThreadPoolExecutor;

import com.espertech.esper.example.benchmark.MarketData;
import com.espertech.esper.example.benchmark.NewsData;
import com.espertech.esper.example.benchmark.UsersData;

/**
 * The ClientConnection handles unmarshalling from the connected client socket and delegates the event to
 * the underlying ESP/CEP engine by using/or not the executor policy.
 * Each ClientConnection manages a throughput statistic (evt/10s) over a 10s batched window
 *
 * @See Server
 * @author Alexandre Vasseur http://avasseur.blogspot.com
 */
public class ClientConnectionOfMy extends Thread {

    static Map<Integer, ClientConnectionOfMy> CLIENT_CONNECTIONS = Collections.synchronizedMap(new HashMap<Integer, ClientConnectionOfMy>());

    public static void dumpStats(int statSec) {
        long totalCount = 0;
        int cnx = 0;
        ClientConnectionOfMy any = null;
        for (ClientConnectionOfMy m : CLIENT_CONNECTIONS.values()) {
            cnx++;
            totalCount += m.countForStatSecLast;
            any = m;
        }
        if (any != null) {
        	System.out.println((float) totalCount / statSec);
            /*System.out.printf("Throughput %.0f (active %d pending %d cnx %d)\n",
                    (float) totalCount / statSec,
                    any.executor == null ? 0 : any.executor.getCorePoolSize(),
                    any.executor == null ? 0 : any.executor.getQueue().size(),
                    cnx
            );*/
        }
    }

    private SocketChannel socketChannel;
    private CEPProvider.ICEPProvider cepProvider;
    //private ThreadPoolExecutor executor;//this guy is shared
    private final int statSec;
    private long countForStatSec = 0;
    private long countForStatSecLast = 0;
    private long lastThroughputTick = System.currentTimeMillis();
    private int myID;
    private static int ID = 0;
    
    public ClientConnectionOfMy(SocketChannel socketChannel,  CEPProvider.ICEPProvider cepProvider, int statSec) {
        super("EsperServer-cnx-" + ID++);
        this.socketChannel = socketChannel;
        this.cepProvider = cepProvider;
        this.statSec = statSec;
        myID = ID - 1;
        CLIENT_CONNECTIONS.put(myID, this);
        this.cepProvider.initPool();
        
    }

    public void run() {
        try {
        	//System.out.println(MarketData.SIZE);
        	ByteBuffer packet = ByteBuffer.allocateDirect(320/8);
        	do {
                if (socketChannel.read(packet) < 0) {
                    System.err.println("Error receiving data from client (got null). Did client disconnect?");
                    break;
                }
                if (packet.hasRemaining()) {
                    ;//System.err.println("partial packet");
                } else {
                	//System.out.println(packet.get(1));
                	if(packet.get(1)==83){
                    packet.flip();
                    final MarketData theEvent = MarketData.fromByteBuffer(packet);
                    long ns = System.nanoTime();
                    cepProvider.sendEventByPool(theEvent);
                    /*long nsDone = System.nanoTime();
                    long msDone = System.currentTimeMillis();
                    StatsHolder.getEngine().update(nsDone - ns);
                    StatsHolder.getEndToEnd().update(msDone - theEvent.getTime());*/
                    
                    //stats
                    countForStatSec++;
                    if (System.currentTimeMillis() - lastThroughputTick > statSec * 1E3) {
                        countForStatSecLast = countForStatSec;
                        countForStatSec = 0;
                        lastThroughputTick = System.currentTimeMillis();//����һ����ǰ�ĺ��룬���������ʵ������1970��1��1��0ʱ��ĺ�����
                    }
                    packet.clear();
                    }
                	else if(packet.get(1)==78){
                        packet.flip();
                        final NewsData theEvent = NewsData.fromByteBuffer(packet);
                        long ns = System.nanoTime();
                        cepProvider.sendEventByPool(theEvent);
                        /*long nsDone = System.nanoTime();
                        long msDone = System.currentTimeMillis();
                        StatsHolder.getEngine().update(nsDone - ns);
                        StatsHolder.getEndToEnd().update(msDone - theEvent.getTime());*/
                        
                        //stats
                        countForStatSec++;
                        if (System.currentTimeMillis() - lastThroughputTick > statSec * 1E3) {
                            countForStatSecLast = countForStatSec;
                            countForStatSec = 0;
                            lastThroughputTick = System.currentTimeMillis();
                        }
                        packet.clear();
                    }
                	
                	else if(packet.get(1)==85){
                        packet.flip();
                        final UsersData theEvent = UsersData.fromByteBuffer(packet);
                        
                        long ns = System.nanoTime();
                        cepProvider.sendEventByPool(theEvent);
                        long nsDone = System.nanoTime();
                        long msDone = System.currentTimeMillis();
                       /* StatsHolder.getEngine().update(nsDone - ns);
                        StatsHolder.getEndToEnd().update(msDone - theEvent.getTime());*/
                       
                        //stats
                        countForStatSec++;
                        if (System.currentTimeMillis() - lastThroughputTick > statSec * 1E3) {
                            countForStatSecLast = countForStatSec;
                            countForStatSec = 0;
                            lastThroughputTick = System.currentTimeMillis();
                        }
                        packet.clear();
                    }
                	
              }
            } while (true);
        } catch (Throwable t) {
            t.printStackTrace();
            System.err.println("Error receiving data from client. Did client disconnect?");
        } finally {
            CLIENT_CONNECTIONS.remove(myID);
            StatsHolder.remove(StatsHolder.getEngine());
            StatsHolder.remove(StatsHolder.getServer());
            StatsHolder.remove(StatsHolder.getEndToEnd());
        }
    }
}
