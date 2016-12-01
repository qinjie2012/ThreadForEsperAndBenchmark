package com.espertech.esper.example.benchmark.server;


import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.LineNumberReader;
import java.lang.management.ManagementFactory;
import java.util.StringTokenizer;


import com.sun.management.OperatingSystemMXBean;


public class SystemUtil {
	private static final int CPUTIME = 500;
	private static final int PERCENT = 100;
	private static final int FAULTLENGTH = 10;


	/**
	 * ���Linux cpuʹ����
	 * @return float efficiency
	 * @throws IOException
	 * @throws InterruptedException
	 */
	public static float getCpuInfo() throws IOException, InterruptedException {
		File file = new File("/proc/stat");
		BufferedReader br = new BufferedReader(new InputStreamReader(
				new FileInputStream(file)));
		StringTokenizer token = new StringTokenizer(br.readLine());
		token.nextToken();
		long user1 = Long.parseLong(token.nextToken() + "");
		long nice1 = Long.parseLong(token.nextToken() + "");
		long sys1 = Long.parseLong(token.nextToken() + "");
		long idle1 = Long.parseLong(token.nextToken() + "");


		Thread.sleep(1000);


		br = new BufferedReader(
				new InputStreamReader(new FileInputStream(file)));
		token = new StringTokenizer(br.readLine());
		token.nextToken();
		long user2 = Long.parseLong(token.nextToken());
		long nice2 = Long.parseLong(token.nextToken());
		long sys2 = Long.parseLong(token.nextToken());
		long idle2 = Long.parseLong(token.nextToken());


		return (float) ((user2 + sys2 + nice2) - (user1 + sys1 + nice1)) * 100
				/ (float) ((user2 + nice2 + sys2 + idle2) - (user1 + nice1
						+ sys1 + idle1));
	}


	// ���cpuʹ����
	public static double getCpuRatioForWindows() {
		try {
			String procCmd = System.getenv("windir")
					+ "\\system32\\wbem\\wmic.exe process get Caption,CommandLine,KernelModeTime,ReadOperationCount,ThreadCount,UserModeTime,WriteOperationCount";


			// ȡ������Ϣ
			long[] c0 = readCpu(Runtime.getRuntime().exec(procCmd));
			Thread.sleep(CPUTIME);


			long[] c1 = readCpu(Runtime.getRuntime().exec(procCmd));


			if ((c0 != null) && (c1 != null)) {
				long idletime = c1[0] - c0[0];
				long busytime = c1[1] - c0[1];


				return Double.valueOf(
						(PERCENT * (busytime) * 1.0) / (busytime + idletime))
						.intValue();
			} else {
				return 0;
			}
		} catch (Exception ex) {
			ex.printStackTrace();


			return 0;
		}
	}


	// ��ȡcpu�����Ϣ
	private static long[] readCpu(final Process proc) {
		long[] retn = new long[2];


		try {
			proc.getOutputStream().close();


			InputStreamReader ir = new InputStreamReader(proc.getInputStream());
			LineNumberReader input = new LineNumberReader(ir);
			String line = input.readLine();


			if ((line == null) || (line.length() < FAULTLENGTH)) {
				return null;
			}


			int capidx = line.indexOf("Caption");
			int cmdidx = line.indexOf("CommandLine");
			int rocidx = line.indexOf("ReadOperationCount");
			int umtidx = line.indexOf("UserModeTime");
			int kmtidx = line.indexOf("KernelModeTime");
			int wocidx = line.indexOf("WriteOperationCount");
			long idletime = 0;
			long kneltime = 0;
			long usertime = 0;


			while ((line = input.readLine()) != null) {
				if (line.length() < wocidx) {
					continue;
				}


				// �ֶγ���˳��Caption,CommandLine,KernelModeTime,ReadOperationCount,
				// ThreadCount,UserModeTime,WriteOperation
				String caption = substring(line, capidx, cmdidx - 1).trim();
				String cmd = substring(line, cmdidx, kmtidx - 1).trim();


				if (cmd.indexOf("wmic.exe") >= 0) {
					continue;
				}


				String s1 = substring(line, kmtidx, rocidx - 1).trim();
				String s2 = substring(line, umtidx, wocidx - 1).trim();


				if (caption.equals("System Idle Process")
						|| caption.equals("System")) {
					if (s1.length() > 0) {
						idletime += Long.valueOf(s1).longValue();
					}


					if (s2.length() > 0) {
						idletime += Long.valueOf(s2).longValue();
					}


					continue;
				}


				if (s1.length() > 0) {
					kneltime += Long.valueOf(s1).longValue();
				}


				if (s2.length() > 0) {
					usertime += Long.valueOf(s2).longValue();
				}
			}


			retn[0] = idletime;
			retn[1] = kneltime + usertime;


			return retn;
		} catch (Exception ex) {
			ex.printStackTrace();
		} finally {
			try {
				proc.getInputStream().close();
			} catch (Exception e) {
				e.printStackTrace();
			}
		}


		return null;
	}


	/**
	 * ����String.subString�Ժ��ִ���������⣨��һ��������Ϊһ���ֽ�)������� �������ֵ��ַ���ʱ�����������ֵ������£�
	 * 
	 * @param src
	 *            Ҫ��ȡ���ַ���
	 * @param start_idx
	 *            ��ʼ���꣨����������)
	 * @param end_idx
	 *            ��ֹ���꣨���������꣩
	 * @return
	 */
	private static String substring(String src, int start_idx, int end_idx) {
		byte[] b = src.getBytes();
		String tgt = "";


		for (int i = start_idx; i <= end_idx; i++) {
			tgt += (char) b[i];
		}


		return tgt;
	}


	// ��ȡ�ڴ�ʹ����
	public static double getMemery() {
		OperatingSystemMXBean osmxb = (OperatingSystemMXBean) ManagementFactory
				.getOperatingSystemMXBean();
		// �ܵ������ڴ�+�����ڴ�
		long totalvirtualMemory = osmxb.getTotalSwapSpaceSize();
		// ʣ��������ڴ�
		long freePhysicalMemorySize = osmxb.getFreePhysicalMemorySize();
		Double compare = (Double) (1 - freePhysicalMemorySize * 1.0
				/ totalvirtualMemory) * 100;
		return compare.intValue();
	}
	
	
	
	public static void main(String[] args) throws IOException, InterruptedException{
		double cpu=SystemUtil.getCpuRatioForWindows();
		double memery=SystemUtil.getMemery();
		System.out.println("cpu�����ʣ�"+cpu);
		System.out.println("�ڴ������ʣ�"+memery);
	}
}