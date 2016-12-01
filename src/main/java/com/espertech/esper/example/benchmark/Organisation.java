package com.espertech.esper.example.benchmark;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Random;
import java.util.Set;

public class Organisation {

    public static final int SIZE;
    public static final int LENGTH=6;

    static {

        String[] organisation = new String[LENGTH];
        ORGANISATION = organisation;

        SIZE = LENGTH * Character.SIZE;
    }

    public static final String ORGANISATION[];
    
    public static String nextOrganisation(String organisation) {
    	//organisation
       	String str="";
        Random RAND = new Random();
    	Map<String,String> map=new HashMap<String,String>();
    	
    	map.put("1", "华尔街日报");
    	map.put("2", "金融中国");
    	map.put("3", "智囊");
    	map.put("4", "时代金融");
    	map.put("5", "第一财经周刊");
    
    	Set set = map.entrySet();
    	Iterator i = set.iterator();
    	int number = RAND.nextInt(3);
    	for(int j=0;j<=number;j++){
    		i.hasNext();
    		Map.Entry<String, String> entry1 = (Map.Entry<String, String>)i.next();
    		str = entry1.getValue();
    	} 
        return str;
    }
}
