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
    	
    	map.put("1", "�������ձ�");
    	map.put("2", "�����й�");
    	map.put("3", "����");
    	map.put("4", "ʱ������");
    	map.put("5", "��һ�ƾ��ܿ�");
    
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
