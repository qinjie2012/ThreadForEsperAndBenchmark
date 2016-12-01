/**************************************************************************************
 * Copyright (C) 2007 Esper Team. All rights reserved.                                *
 * http://esper.codehaus.org                                                          *
 * ---------------------------------------------------------------------------------- *
 * The software in this package is published under the terms of the GPL license       *
 * a copy of which has been included with this distribution in the license.txt file.  *
 **************************************************************************************/
package com.espertech.esper.example.benchmark;

import java.util.Random;



public class UsersSymbols {

    private static final Random RAND = new Random();

    static {//生成UsersData中usersId的部分代码：
    	 String[] usersymbols = new String[1000];
         for (int i = 0; i < 1000; i++) {
        	 usersymbols[i] = "U" + i;
             while (usersymbols[i].length() < 4) {
            	 usersymbols[i] += "A";
             }
         }
         USERSSYMBOLS = usersymbols;
    }

    public static final String USERSSYMBOLS[];
    
    
    static {//生成UsersData中newsId的部分代码：
        String[] newsId = new String[1000];
        for (int i = 0; i < 1000; i++) {
        	newsId[i] = "N" + i;
            while (newsId[i].length() < 4) {
            	newsId[i] += "A";
            }
        }
        NEWSID = newsId;
    }
    
    public static final String NEWSID[];
    
    
    static {//生成UsersData中ticker的部分代码：
        String[] usersticker = new String[1000];
        for (int i = 0; i < 1000; i++) {
        	usersticker[i] = "S" + i;
            while (usersticker[i].length() < 4) {
            	usersticker[i] += "A";
            }
        }
        USERSTICKER = usersticker;
    }
    
    public static final String USERSTICKER[];
    
    

    public static String nextUsersId(int max){
    	int i = new Random().nextInt(1000);
    	String usersId = "U" + i;
        while (usersId.length() < 4) {
        	usersId += "A";
        }
    	return usersId;
    }
    
    public static String nextNewsId(int max){
    	int i = new Random().nextInt(1000);
    	String newsId = "N" + i;
        while (newsId.length() < 4) {
            newsId += "A";
        }
    	return newsId;
    }
    
   
    public static long nextHoldvolume() {
        long result = RAND.nextLong();
        return result;
    }
    
}
