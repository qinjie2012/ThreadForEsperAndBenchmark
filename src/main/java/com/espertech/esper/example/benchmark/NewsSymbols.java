/**************************************************************************************
 * Copyright (C) 2007 Esper Team. All rights reserved.                                *
 * http://esper.codehaus.org                                                          *
 * ---------------------------------------------------------------------------------- *
 * The software in this package is published under the terms of the GPL license       *
 * a copy of which has been included with this distribution in the license.txt file.  *
 **************************************************************************************/
package com.espertech.esper.example.benchmark;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Random;
import java.util.Set;


public class NewsSymbols {

    private static final Random RAND = new Random();
  //  public static final int SIZE;
  //  public static final int LENGTH;

    static {//生成newsId的部分代码：
       /* int newssymbolcount = Integer.parseInt(System.getProperty("esper.benchmark.symbol", "1000"));
        LENGTH = ("" + newssymbolcount).length();
        String[] newssymbols = new String[newssymbolcount];
        for (int i = 0; i < newssymbols.length; i++) {
            newssymbols[i] = "N" + i;
            while (newssymbols[i].length() < LENGTH) {
                newssymbols[i] += "A";
            }
        }

        NEWSSYMBOLS = newssymbols;
        SIZE = LENGTH * Character.SIZE;*/
    	 String[] newssymbols = new String[1000];
         for (int i = 0; i < 1000; i++) {
             newssymbols[i] = "N" + i;
             while (newssymbols[i].length() < 4) {
                 newssymbols[i] += "A";
             }
         }
         NEWSSYMBOLS = newssymbols;
    }

    public static final String NEWSSYMBOLS[];
    
    
    
    static {//生成NewsData中ticker的部分代码：
        String[] newsticker = new String[1000];
        for (int i = 0; i < 1000; i++) {
            newsticker[i] = "S" + i;
            while (newsticker[i].length() < 4) {
                newsticker[i] += "A";
            }
        }
        NEWSTICKER = newsticker;
    }
    
    public static final String NEWSTICKER[];
    
    

    public static String nextNewsId(int max){
    	int i = new Random().nextInt(1000);
    	String newsId = "N" + i;
        while (newsId.length() < 4) {
            newsId += "A";
        }
    	return newsId;
    }
    
    
    
    public static String nextDate(int max){
        //date
        SimpleDateFormat ft = new SimpleDateFormat("HH:mm:ss");
    	Date d =new Date();
    	String date =ft.format(d);
    	//String date = "SJ";
    	return date;
    }
    public static String nextOrganisation(int max){
    	//organisation
       	String str="";
        Random RAND = new Random();
    	Map<String,String> map=new HashMap<String,String>();
    	
    /*	map.put("1", "华尔街日报");
    	map.put("2", "金融中国");
    	map.put("3", "智囊");
    	map.put("4", "时代金融");
    	map.put("5", "第一财经周刊");*/
    	
    	map.put("1", "AA");
    	map.put("2", "BB");
    	map.put("3", "CC");
    	map.put("4", "DD");
    	map.put("5", "EE");
    	map.put("6", "FF");
    
    	Set set = map.entrySet();
    	Iterator i = set.iterator();
    	int number = RAND.nextInt(6);
    	for(int j=0;j<=number;j++){
    		i.hasNext();
    		Map.Entry<String, String> entry1 = (Map.Entry<String, String>)i.next();
    		str = entry1.getValue();
    	} 
        return str;
    }
}
