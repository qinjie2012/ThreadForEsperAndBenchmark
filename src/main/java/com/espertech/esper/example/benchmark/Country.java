package com.espertech.esper.example.benchmark;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Random;
import java.util.Set;

public class Country {

    public static final int SIZE;
    public static final int LENGTH=6;

    static {

        String[] country = new String[LENGTH];
/*        country[0]="C";
        country[1]="N";*/
        COUNTRY = country;

        SIZE = LENGTH * Character.SIZE;
    }

    public static final String COUNTRY[];
    
    public static String nextCountry(String country) {
        String str="";
        Random RAND = new Random();
    	Map<String,String> map=new HashMap<String,String>();
    	
    	map.put("1", "Canada");
    	map.put("2", "Poland");
    	map.put("3", "Norway");
    	map.put("4", "Greece");
    	map.put("5", "Sweden");
    	map.put("6", "Russia");

    	Set set = map.entrySet();
    	Iterator i = set.iterator();
    	int number = RAND.nextInt(6);
    	for(int j=0;j<=number;j++){
    		i.hasNext();
    		Map.Entry<String, String> entry1 = (Map.Entry<String, String>)i.next();
    		str= entry1.getValue();
    	}
        return str;
    }
}
