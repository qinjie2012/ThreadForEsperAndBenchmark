/**************************************************************************************
 * Copyright (C) 2007 Esper Team. All rights reserved.                                *
 * http://esper.codehaus.org                                                          *
 * ---------------------------------------------------------------------------------- *
 * The software in this package is published under the terms of the GPL license       *
 * a copy of which has been included with this distribution in the license.txt file.  *
 **************************************************************************************/
package com.espertech.esper.example.benchmark;

import java.nio.ByteBuffer;
import java.nio.CharBuffer;

/**
 * The actual event.
 * The time property (ms) is the send time from the client sender, and can be used for end to end latency providing client(s)
 * and server OS clocks are in sync.
 * The inTime property is the unmarshal (local) time (ns).
 *
 * @author Alexandre Vasseur http://avasseur.blogspot.com
 */
public class UsersData implements InTime{

    public final static int SIZE = 320 ;
    static {
        System.out.println("UsersData event = " + SIZE + " bit = " + SIZE/8 + " bytes");
        System.out.println("  100 Mbit/s <==> " + (int) (100*1024*1024/SIZE/1000) + "k evt/s");
        System.out.println("    1 Gbit/s <==> " + (int) (1024*1024*1024/SIZE/1000) + "k evt/s");
    }
    
    private  String usersId;
    private  String ticker;
    private  String newsId;
    private  long  holdvolume;
    
    private long time;//ms
    private final long inTime;


    public UsersData(String usersId, String newsId, String ticker, long holdvolume) {
        this();
        this.usersId = usersId;
        this.newsId = newsId;
        this.ticker = ticker;
        this.holdvolume = holdvolume;
    }

    private UsersData() {
    	this.inTime = System.nanoTime();
    }
    
    
    public String getUsersId() {
        return usersId;
    }
    
    public void setUsersId(String usersId) {
    	this.usersId = usersId;
    }
    
    public String getNewsId() {
        return newsId;
    }

    public void setNewsId(String newsId) {
        this.newsId = newsId;
    }

    public String getTicker() {
        return ticker;
    }

    public void setTicker(String ticker) {
        this.ticker = ticker;
    }
    
    public long getHoldvolume(){
    	return holdvolume;
    }
    
    public void setHoldvolume(long holdvolume){
    	this.holdvolume = holdvolume;
    }
    
    public long getTime() {
        return time;
    }


    public long getInTime() {
        return inTime;
    }


    public void toByteBuffer(ByteBuffer b) {
        //usersId��newsId��ticker
        CharBuffer cb = b.asCharBuffer();
        cb.put(usersId);
        cb.put(newsId);
        cb.put(ticker);
        b.position(b.position() + cb.position() * 2);
        //price, volume
        b.putLong(holdvolume);
        //inTime
        b.putLong(System.currentTimeMillis());
    }
    
    

    public static UsersData fromByteBuffer(ByteBuffer byteBuffer) {
        UsersData ud = new UsersData();   
        //usersId
        char[] usersId = new char[4];
        CharBuffer cb = byteBuffer.asCharBuffer();
        cb.get(usersId);
        ud.setUsersId(String.valueOf(usersId));
        //newsId
        char[] newsId = new char[4];
        cb.get(newsId);
        ud.setNewsId(String.valueOf(newsId)); 
        //ticker
        char[] ticker = new char[4];
        cb.get(ticker);
        ud.setTicker(String.valueOf(ticker));
        //holdvolume
        byteBuffer.position(byteBuffer.position() + cb.position() * 2);
        ud.setHoldvolume(byteBuffer.getLong());
        return ud;
    }

    public String toString() {
        return usersId+" : "+newsId+" : "+ticker+" : "+holdvolume;
    }

    public Object clone() throws CloneNotSupportedException {
        return new UsersData(usersId, newsId, ticker, holdvolume);
    }
}


