/**************************************************************************************
 * Copyright (C) 2007 Esper Team. All rights reserved.                                *
 * http://esper.codehaus.org                                                          *
 * ---------------------------------------------------------------------------------- *
 * The software in this package is published under the terms of the GPL license       *
 * a copy of which has been included with this distribution in the license.txt file.  *
 **************************************************************************************/
package com.espertech.esper.example.benchmark.client;


import com.espertech.esper.example.benchmark.Country;
import com.espertech.esper.example.benchmark.Symbols;
import com.espertech.esper.example.benchmark.NewsSymbols;
import com.espertech.esper.example.benchmark.MarketData;
import com.espertech.esper.example.benchmark.NewsData;
import com.espertech.esper.example.benchmark.UsersData;
import com.espertech.esper.example.benchmark.UsersSymbols;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.util.Random;

/**
 * A thread that sends market data (symbol, volume, price) at the target rate to the remote host
 *
 * @author Alexandre Vasseur http://avasseur.blogspot.com
 */
/*public class MarketClient extends Thread {*/
public class NewsClient extends Thread {

    private Client client;
    private MarketData market[];
    private NewsData news[];
    private UsersData users[];
    
	int number = 3;

  /*  public MarketClient(Client client) {
        this.client = client;
        market = new MarketData[Symbols.SYMBOLS.length];
        for (int i = 0; i < market.length; i++) {
            market[i] = new MarketData(Symbols.SYMBOLS[i], Symbols.nextPrice(10), Symbols.nextVolume(10));
        }
        System.out.printf("MarketData with %d symbols\n", market.length);
    }*/
    
    
    public NewsClient(Client client) {
        this.client = client;
        if(number==1){
         	market = new MarketData[Symbols.SYMBOLS.length];
            for (int i = 0; i < market.length; i++) {
                market[i] = new MarketData(Symbols.SYMBOLS[i], Symbols.nextPrice(10), Symbols.nextVolume(10), Country.nextCountry(""));
            }
            System.out.printf("MarketData with %d symbols\n", market.length);
        }
        else if(number==2){
        	market = new MarketData[Symbols.SYMBOLS.length];
            for (int i = 0; i < market.length; i++) {
                market[i] = new MarketData(Symbols.SYMBOLS[i], Symbols.nextPrice(10), Symbols.nextVolume(10), Country.nextCountry(""));
            }
            System.out.printf("MarketData with %d symbols\n", market.length);
            
            news = new NewsData[NewsSymbols.NEWSSYMBOLS.length];
            for (int i = 0; i < news.length; i++) {
                news[i] = new NewsData(NewsSymbols.NEWSSYMBOLS[i], NewsSymbols.NEWSTICKER[i], NewsSymbols.nextDate(10), NewsSymbols.nextOrganisation(10));
            }
            System.out.printf("NewsData with %d symbols\n", news.length);
        }
        else if(number==3){
        	market = new MarketData[Symbols.SYMBOLS.length];
            for (int i = 0; i < market.length; i++) {
                market[i] = new MarketData(Symbols.SYMBOLS[i], Symbols.nextPrice(10), Symbols.nextVolume(10), Country.nextCountry(""));
            }
            System.out.printf("MarketData with %d symbols\n", market.length);
            
            news = new NewsData[NewsSymbols.NEWSSYMBOLS.length];
            for (int i = 0; i < news.length; i++) {
                news[i] = new NewsData(NewsSymbols.NEWSSYMBOLS[i], NewsSymbols.NEWSTICKER[i], NewsSymbols.nextDate(10), NewsSymbols.nextOrganisation(10));
            }
            System.out.printf("NewsData with %d symbols\n", news.length);
            
            users = new UsersData[UsersSymbols.USERSSYMBOLS.length];
            for (int i = 0; i < news.length; i++) {
                users[i] = new UsersData(UsersSymbols.USERSSYMBOLS[i], UsersSymbols.NEWSID[i], UsersSymbols.USERSTICKER[i], UsersSymbols.nextHoldvolume());
            }
            System.out.printf("UsersData with %d symbols\n", users.length);
        }      
    }
    

    public void run() {
        SocketChannel socketChannel = null;
        try {
            socketChannel = SocketChannel.open(new InetSocketAddress(client.host, client.port));
            System.out.printf("Client connected to %s:%d, rate %d msg/s\n", client.host, client.port, client.rate);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        MarketData market[] = this.market;
        int eventPer50ms = client.rate / 20;
        int tickerIndex = 0;
        int countLast5s = 0;
        int sleepLast5s = 0;
        long lastThroughputTick = System.currentTimeMillis();
        try{
        	ByteBuffer byteBuffer = ByteBuffer.allocateDirect(MarketData.SIZE / 8);
        	do {
        		number = new Random().nextInt(3) + 1; //产生1~3的随机数，用来识别那个事件流
        		long ms = System.currentTimeMillis();
                if(number==1){
                    for (int i = 0; i < eventPer50ms; i++) {
                        tickerIndex = tickerIndex % Symbols.SYMBOLS.length;
                        MarketData md = market[tickerIndex++];
                        md.setPrice(Symbols.nextPrice(md.getPrice()));
                        md.setVolume(Symbols.nextVolume(10));
                        

                        byteBuffer.clear();
                        md.toByteBuffer(byteBuffer);
                        byteBuffer.flip();
                        socketChannel.write(byteBuffer);

                        countLast5s++;

                        // info
                        if (System.currentTimeMillis() - lastThroughputTick > 5 * 1E3) {
                            System.out.printf("Sent %d in %d(ms) avg ns/msg %.0f(ns) avg %d(msg/s) sleep %d(ms)\n",
                                    countLast5s,
                                    System.currentTimeMillis() - lastThroughputTick,
                                    (float) 1E6 * countLast5s / (System.currentTimeMillis() - lastThroughputTick),
                                    countLast5s / 5,
                                    sleepLast5s
                            );
                            countLast5s = 0;
                            sleepLast5s = 0;
                            lastThroughputTick = System.currentTimeMillis();
                        }
                    }

                    // rate adjust
                    if (System.currentTimeMillis() - ms < 50) {
                        // lets avoid sleeping if == 1ms, lets account 3ms for interrupts
                        long sleep = Math.max(1, (50 - (System.currentTimeMillis() - ms) - 3));
                        sleepLast5s += sleep;
                        Thread.sleep(sleep);
                    }
                }
                else if(number==2){
                        for (int i = 0; i < eventPer50ms; i++) {
                            tickerIndex = tickerIndex % NewsSymbols.NEWSSYMBOLS.length;
                            NewsData md = news[tickerIndex++];
                            md.setNewsId(NewsSymbols.nextNewsId(10));
                            md.setDate(NewsSymbols.nextDate(10));
                            md.setOrganisation(NewsSymbols.nextOrganisation(10));
                            

                            byteBuffer.clear();
                            md.toByteBuffer(byteBuffer);
                            byteBuffer.flip();
                            socketChannel.write(byteBuffer);

                            countLast5s++;

                            // info
                            if (System.currentTimeMillis() - lastThroughputTick > 5 * 1E3) {
                                System.out.printf("Sent %d in %d(ms) avg ns/msg %.0f(ns) avg %d(msg/s) sleep %d(ms)\n",
                                        countLast5s,
                                        System.currentTimeMillis() - lastThroughputTick,
                                        (float) 1E6 * countLast5s / (System.currentTimeMillis() - lastThroughputTick),
                                        countLast5s / 5,
                                        sleepLast5s
                                );
                                countLast5s = 0;
                                sleepLast5s = 0;
                                lastThroughputTick = System.currentTimeMillis();
                            }
                        }

                        // rate adjust
                        if (System.currentTimeMillis() - ms < 50) {
                            // lets avoid sleeping if == 1ms, lets account 3ms for interrupts
                            long sleep = Math.max(1, (50 - (System.currentTimeMillis() - ms) - 3));
                            sleepLast5s += sleep;
                            Thread.sleep(sleep);
                        }
                  }
                else if(number==3){
                    for (int i = 0; i < eventPer50ms; i++) {
                        tickerIndex = tickerIndex % UsersSymbols.USERSSYMBOLS.length;
                        UsersData ud = users[tickerIndex++];
                        ud.setUsersId(UsersSymbols.nextUsersId(10));
                        ud.setNewsId(UsersSymbols.nextNewsId(10));
                        ud.setHoldvolume(UsersSymbols.nextHoldvolume());

                        byteBuffer.clear();
                        ud.toByteBuffer(byteBuffer);
                        byteBuffer.flip();
                        socketChannel.write(byteBuffer);

                        countLast5s++;

                        // info
                        if (System.currentTimeMillis() - lastThroughputTick > 5 * 1E3) {
                            System.out.printf("Sent %d in %d(ms) avg ns/msg %.0f(ns) avg %d(msg/s) sleep %d(ms)\n",
                                    countLast5s,
                                    System.currentTimeMillis() - lastThroughputTick,
                                    (float) 1E6 * countLast5s / (System.currentTimeMillis() - lastThroughputTick),
                                    countLast5s / 5,
                                    sleepLast5s
                            );
                            countLast5s = 0;
                            sleepLast5s = 0;
                            lastThroughputTick = System.currentTimeMillis();
                        }
                    }

                    // rate adjust
                    if (System.currentTimeMillis() - ms < 50) {
                        // lets avoid sleeping if == 1ms, lets account 3ms for interrupts
                        long sleep = Math.max(1, (50 - (System.currentTimeMillis() - ms) - 3));
                        sleepLast5s += sleep;
                        Thread.sleep(sleep);
                    }
              }
            } while (true);
        }
        catch (Throwable t) {
            t.printStackTrace();
            System.err.println("Error sending data to server. Did server disconnect?");
        }
    }
}
