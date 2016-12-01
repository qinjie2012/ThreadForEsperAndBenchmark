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
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.Random;

/**
 * The actual event.
 * The time property (ms) is the send time from the client sender, and can be used for end to end latency providing client(s)
 * and server OS clocks are in sync.
 * The inTime property is the unmarshal (local) time (ns).
 *
 * @author Alexandre Vasseur http://avasseur.blogspot.com
 */
public class NewsData implements InTime{

    public final static int SIZE = 320 ;
    static {
        System.out.println("NewsData event = " + SIZE + " bit = " + SIZE/8 + " bytes");
        System.out.println("  100 Mbit/s <==> " + (int) (100*1024*1024/SIZE/1000) + "k evt/s");
        System.out.println("    1 Gbit/s <==> " + (int) (1024*1024*1024/SIZE/1000) + "k evt/s");
    }
    
    private  String newsId;
    private  String ticker;
    private  String date;
    private  String organisation;
    
    private long time;//ms
    private final long inTime;


    public NewsData(String newsId, String ticker, String date, String organisation) {
        this();
        this.newsId = newsId;
        this.ticker = ticker;
        this.date = date;
        this.organisation = organisation;
    }

    private NewsData() {
    	this.inTime = System.nanoTime();
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
    
    public String getDate() {
        return date;
    }

    public void setDate(String date) {
        this.date = date;
    }
    
    public String getOrganisation() {
        return organisation;
    }
    
    public void setOrganisation(String organisation) {
        this.organisation = organisation;
    }
    
    public long getTime() {
        return time;
    }

    private void setTime(long time) {
        this.time = time;
    }

    public long getInTime() {
        return inTime;
    }


    public void toByteBuffer(ByteBuffer b) {
        //newsId��ticker��date��organisation
        CharBuffer cb = b.asCharBuffer();
        cb.put(newsId);
        cb.put(ticker);//we know ticker is a fixed length string
        System.out.println(date.length());
        cb.put(date);
        cb.put(organisation);
        b.position(b.position() + cb.position() * 2);
        //inTime
        b.putLong(System.currentTimeMillis());
    }
    
    

    public static NewsData fromByteBuffer(ByteBuffer byteBuffer) {
        NewsData nd = new NewsData();   
        //newsId
        char[] newsId = new char[4];
        CharBuffer cb = byteBuffer.asCharBuffer();
        cb.get(newsId);
        nd.setNewsId(String.valueOf(newsId)); 
        //ticker
        char[] ticker = new char[4];
        cb.get(ticker);
        nd.setTicker(String.valueOf(ticker));
        //date
        char[] date = new char[8];
        cb.get(date);
        nd.setDate(String.valueOf(date));
        //organisation
        char[] organisation = new char[2];
        cb.get(organisation);
        nd.setOrganisation(String.valueOf(organisation));
        return nd;
    }

    public String toString() {
        return newsId+" : "+ticker+" : "+date+" : "+organisation;
    }

    public Object clone() throws CloneNotSupportedException {
        return new NewsData(newsId, ticker, date, organisation);
    }
}


