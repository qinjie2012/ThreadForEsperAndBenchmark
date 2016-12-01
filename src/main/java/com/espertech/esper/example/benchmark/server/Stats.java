/**************************************************************************************
 * Copyright (C) 2007 Esper Team. All rights reserved.                                *
 * http://esper.codehaus.org                                                          *
 * ---------------------------------------------------------------------------------- *
 * The software in this package is published under the terms of the GPL license       *
 * a copy of which has been included with this distribution in the license.txt file.  *
 **************************************************************************************/
package com.espertech.esper.example.benchmark.server;

import java.util.concurrent.atomic.AtomicBoolean;

import java.io.*;

/**
 * A Stats instance gathers percentile based on a given histogram
 * This class is thread unsafe.
 * @see com.espertech.esper.example.benchmark.server.StatsHolder for thread safe access
 * Use createAndMergeFrom(proto) for best effort merge of this instance into the proto instance
 * (no read / write lock is performed so the actual counts are a best effort)
 *
 * @author Alexandre Vasseur http://avasseur.blogspot.com
 */
public class Stats {

    private AtomicBoolean mustReset = new AtomicBoolean(false);//������ԭ�ӷ�ʽ���µ� boolean ֵ��

    final public String name;
    final public String unit;
    private long count;
    private double avg;

    private int[] histogram;
    private long[] counts;
    //Java1.5�ṩ��һ����varargs�Ĺ��ܣ����ǿɱ䳤�ȵĲ����е�������main�����Ĳ���String[] args������������������ʱargs�����ǿɱ�ġ�һ��������ֻ����һ��ʡ�Ժ�
    public Stats(String name, String unit, int... hists) {//10, 20, (20+ implicit)
        this.name = name;
        this.unit = unit;
        histogram = new int[hists.length + 1];//we add one slot for the implicit 20+
        System.arraycopy(hists, 0, histogram, 0, hists.length);//��hists������histogram��
        histogram[histogram.length - 1] = hists[hists.length - 1] + 1;
        counts = new long[histogram.length];
        for (int i = 0; i < counts.length; i++)
            counts[i] = 0;
    }

    /**
     * Use this method to merge this stat instance into a prototype one (for thread safe read only snapshoting)
     */
    public static Stats createAndMergeFrom(Stats model) {
        Stats r = new Stats(model.name, model.unit, 0);
        r.histogram = new int[model.histogram.length];
        System.arraycopy(model.histogram, 0, r.histogram, 0, model.histogram.length);
        r.counts = new long[model.histogram.length];

        r.merge(model);
        return r;
    }

    public void update(long ns) {
        if (mustReset.compareAndSet(true, false))//compareAndSet(boolean expect, boolean update) ���ǰֵ == Ԥ��ֵ������ԭ�ӷ�ʽ����ֵ����Ϊ��ĸ���ֵ��
            internal_reset();

        count++;
        avg = (avg*(count-1) + ns)/count;
        if (ns >= histogram[histogram.length - 1]) {
            counts[counts.length - 1]++;
        } else {
            int index = 0;
            for (int level : histogram) {
                if (ns < level) {
                    counts[index]++;
                    break;
                }
                index++;
            }
        }
    }

    public void dump() {
		/*try{
			File file = new File("D://StatsResult.txt");
			if(!file.exists()){
				file.createNewFile();
			}
			
			//FileWriter fw = new FileWriter(file.getAbsoluteFile(),true);
			//BufferedWriter bw = new BufferedWriter(fw);
			FileOutputStream fop = new FileOutputStream(file,true);
			OutputStreamWriter writer = new OutputStreamWriter(fop);
			writer.append("������������������������������������������������������\r\n");
			writer.append("ͳ�����������ʾ��\r\n");
			writer.append("CPU�����ʣ�"+cpu+"%\r\n");
			writer.append("�ڴ������ʣ�"+memery+"%\r\n");
			SysInfo s=new SysInfo();
	        s.testCpuPerc();
	        s.getPhysicalMemory();
			writer.append("---Stats - " + name + " (unit: " + unit + ")\r\n");
			writer.append("  Avg: "+avg+" #"+count+"\r\n" );
			int index = 0;
	        long lastLevel = 0;
	        long occurCumul = 0;
	        for (long occur : counts) {
	            occurCumul += occur;//cumulate����
	            if (index != counts.length - 1) {
	            	writer.append(lastLevel+" "+histogram[index]+" "+(float) occur / count * 100+" "+(float) occurCumul  / count * 100+" "+occur+"\r\n");
	                lastLevel = histogram[index];
	            } else {
	            	writer.append(lastLevel+" "+(float)occur /count * 100+" "+100f+" "+occur+"\r\n");
	            }
	            index++;
	        }
	        writer.close();
	        fop.close();
		}catch(IOException e){
			e.printStackTrace();
		}
		*/
		
		//��������
		/*try{
			File file = new File("D://StatsResult.txt");
			if(!file.exists()){
				file.createNewFile();
			}
			
			FileWriter fw = new FileWriter(file.getAbsoluteFile(),true);
			BufferedWriter bw = new BufferedWriter(fw);
			bw.write("������������������������������������������������������\r\n");
			bw.write("ͳ�����������ʾ��\r\n");
			bw.write("CPU�����ʣ�"+cpu+"%\r\n");
			bw.write("�ڴ������ʣ�"+memery+"%\r\n");
			//SysInfo s=new SysInfo();
	        //s.testCpuPerc();
	        //s.getPhysicalMemory();
			bw.write("---Stats - " + name + " (unit: " + unit + ")\r\n");
			bw.write("  Avg: "+avg+" #"+count+"\r\n" );
			int index = 0;
	        long lastLevel = 0;
	        long occurCumul = 0;
	        for (long occur : counts) {
	            occurCumul += occur;//cumulate����
	            if (index != counts.length - 1) {
	            	bw.write(lastLevel+" "+histogram[index]+" "+(float) occur / count * 100+" "+(float) occurCumul  / count * 100+" "+occur+"\r\n");
	                lastLevel = histogram[index];
	            } else {
	                bw.write(lastLevel+" "+(float)occur /count * 100+" "+100f+" "+occur+"\r\n");
	            }
	            index++;
	        }
	        bw.close();
		}catch(IOException e){
			e.printStackTrace();
		}*/
        
		try{
    		FileOutputStream fos = new FileOutputStream("StatsResult1.txt",true);
    		PrintStream ps = new PrintStream(fos);
    		System.setOut(ps);
    		
    		
    		SysInfo s=new SysInfo();
    		//s.testCpu();//�ܵ�CPUʹ����
    		s.getPhysicalMemory();//物理内存使用情况
            s.testCpuPerc();//CPU使用率
//            System.out.print(" ");
            
    		
            int index = 0;
            long lastLevel = 0;
            long occurCumul = 0;
            for (long occur : counts) {
                occurCumul += occur;//cumulate����
                if (index != counts.length) {
                	//System.out.println((float) occur / count * 100+"   "+(float) occurCumul  / count * 100+"  "+occur);
                	System.out.printf("%4.2f%% ", (float) occur / count * 100);
                    lastLevel = histogram[index];
                }
                index++;
            }
    	}catch(FileNotFoundException e){
    		e.printStackTrace();
    	}
    }

    public void merge(Stats stats) {
        // we assume same histogram - no check done here
        count += stats.count;
        avg = ((avg * count) + (stats.avg * stats.count)) / (count + stats.count);
        for (int i = 0; i < counts.length; i++) {
            counts[i] += stats.counts[i];
        }
    }

    private void internal_reset() {
        count = 0;
        avg = 0;
        for (int i = 0; i < counts.length; i++)
            counts[i] = 0;
    }

    public void reset() {
        mustReset.set(true);
    }

    public static void main(String[] args) {
        Stats stats = new Stats("a", "any", 10, 20);
        stats.update(1);
        stats.update(2);
        stats.update(10);
        stats.update(15);
        stats.update(25);
       // stats.dump();

        Stats stats2 = new Stats("b", "any", 10, 20);
        stats2.update(1);
        stats.merge(stats2);
        stats.dump();

        long l = 100;
        long l2 = 3;
        System.out.println(""+ (float) l/l2);
        System.out.printf("%15.4f", (float) l/l2);
    }
}
