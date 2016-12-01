package com.espertech.esper.example.benchmark.server;


import java.net.InetAddress;
import java.net.UnknownHostException;
import java.text.DecimalFormat;

import java.io.*;

import org.hyperic.sigar.CpuInfo;
import org.hyperic.sigar.CpuPerc;
import org.hyperic.sigar.FileSystem;
import org.hyperic.sigar.FileSystemUsage;
import org.hyperic.sigar.Mem;
import org.hyperic.sigar.NetFlags;
import org.hyperic.sigar.NetInterfaceConfig;
import org.hyperic.sigar.NetInterfaceStat;
import org.hyperic.sigar.OperatingSystem;
import org.hyperic.sigar.Sigar;
import org.hyperic.sigar.SigarException;
import org.hyperic.sigar.SigarNotImplementedException;
import org.hyperic.sigar.Swap;

public class SysInfo {

    public static void main(String [] args) throws Exception{
        SysInfo s =new SysInfo();
        System.out.println("CPU������"+s.getCpuCount());
        s.getCpuTotal();
        s.testCpuPerc();
        s.testCpu();
        s.getPhysicalMemory();
        s.testWho();
        s.testFileSystemInfo();
        s.testGetOSInfo();
    }
    /**
     * 1.CPU��Դ��Ϣ
     */
    
    // a)CPU��������λ������
    public static int getCpuCount() throws SigarException {
        Sigar sigar = new Sigar();
        try {
            return sigar.getCpuInfoList().length;
        } finally {
            sigar.close();
        }
    }

    // b)CPU����������λ��HZ����CPU�������Ϣ
    public void getCpuTotal() {
        Sigar sigar = new Sigar();
        CpuInfo[] infos;
        try {
            infos = sigar.getCpuInfoList();
            for (int i = 0; i < infos.length; i++) {// �����ǵ���CPU���Ƕ�CPU������
                CpuInfo info = infos[i];
                System.out.println("CPU������:" + info.getMhz());// CPU������MHz
                System.out.println("���CPU��������" + info.getVendor());// ���CPU���������磺Intel
                System.out.println("CPU�����" + info.getModel());// ���CPU������磺Celeron
                System.out.println("����洢��������" + info.getCacheSize());// ����洢������
                System.out.println("**************");
            }
        } catch (SigarException e) {
            e.printStackTrace();
        }
    }

    // c)CPU���û�ʹ������ϵͳʹ��ʣ�������ܵ�ʣ�������ܵ�ʹ��ռ�����ȣ���λ��100%��
    public void testCpuPerc() {
        Sigar sigar = new Sigar();
        // ��ʽһ����Ҫ�����һ��CPU�����
       /* CpuPerc cpu;
        try {
            cpu = sigar.getCpuPerc();
            printCpuPerc(cpu);
        } catch (SigarException e) {
            e.printStackTrace();
        }*/
        // ��ʽ���������ǵ���CPU���Ƕ�CPU������
        CpuPerc cpuList[] = null;
        try {
            cpuList = sigar.getCpuPercList();
        } catch (SigarException e) {
            e.printStackTrace();
            return;
        }
        for (int i = 0; i < cpuList.length; i++) {
        	//System.out.println("��"+(i+1)+"��CPUʹ�������");
            printCpuPerc(cpuList[i]);
        }
    }
    
    public void testCpu() {
        Sigar sigar = new Sigar();
        // ��ʽһ����Ҫ�����һ��CPU�����
        CpuPerc cpu;
        try {
            cpu = sigar.getCpuPerc();
            printCpuPerc(cpu);
        } catch (SigarException e) {
            e.printStackTrace();
        }
    }


    private void printCpuPerc(CpuPerc cpu) {
    	System.out.print(CpuPerc.format(cpu.getCombined())+" ");//cpuʹ����
        /*System.out.println("�û�ʹ����:" + CpuPerc.format(cpu.getUser()));// �û�ʹ����
        System.out.println("ϵͳʹ����:" + CpuPerc.format(cpu.getSys()));// ϵͳʹ����
        System.out.println("��ǰ�ȴ���:" + CpuPerc.format(cpu.getWait()));// ��ǰ�ȴ���
        System.out.println("��ǰ������:" + CpuPerc.format(cpu.getIdle()));// ��ǰ������
        System.out.println("�ܵ�ʹ����:" + CpuPerc.format(cpu.getCombined()));// �ܵ�ʹ����
        System.out.println("**************");*/
    }

    /**
     * 2.�ڴ���Դ��Ϣ
     *
     */
    
    public void getPhysicalMemory() {
        // a)�����ڴ���Ϣ
        DecimalFormat df = new DecimalFormat("#0.00");
        Sigar sigar = new Sigar();
        Mem mem;
        try {
            mem = sigar.getMem();
            System.out.print(100 * mem.getUsed()/mem.getTotal() + "% ");//�ڴ�ʹ����
            // �ڴ�����
           /* System.out.println("�ڴ�������" + df.format((float)mem.getTotal() / 1024/1024/1024) + "G");
            // ��ǰ�ڴ�ʹ����
            System.out.println("��ǰ�ڴ�ʹ������" + df.format((float)mem.getUsed() / 1024/1024/1024) + "G");
            
            // ��ǰ�ڴ�ʹ����
            System.out.println("��ǰ�ڴ�ʹ���ʣ� " + 100 * mem.getUsed()/mem.getTotal() + "%");
            
            // ��ǰ�ڴ�ʣ����
            System.out.println("��ǰ�ڴ�ʣ������" + df.format((float)mem.getFree() / 1024/1024/1024) + "G");
            // b)ϵͳҳ���ļ���������Ϣ
            Swap swap = sigar.getSwap();
            // ����������
            System.out.println("������������" + df.format((float)swap.getTotal() / 1024/1024/1024) + "G");
            // ��ǰ������ʹ����
            System.out.println("��ǰ������ʹ������" + df.format((float)swap.getUsed() / 1024/1024/1024) + "G");
            // ��ǰ������ʣ����
            System.out.println("��ǰ������ʣ������" + df.format((float)swap.getFree() / 1024/1024/1024) + "G");*/
        } catch (SigarException e) {
            e.printStackTrace();
        }
    }

    /**
     * 3.����ϵͳ��Ϣ
     * 
     */
    
    // a)ȡ����ǰ����ϵͳ�����ƣ�
    public String getPlatformName() {
        String hostname = "";
        try {
            hostname = InetAddress.getLocalHost().getHostName();
        } catch (Exception exc) {
            Sigar sigar = new Sigar();
            try {
                hostname = sigar.getNetInfo().getHostName();
            } catch (SigarException e) {
                hostname = "localhost.unknown";
            } finally {
                sigar.close();
            }
        }
        return hostname;
    }

    // b)ȡ��ǰ����ϵͳ����Ϣ
    public void testGetOSInfo() {
        OperatingSystem OS = OperatingSystem.getInstance();
        // ����ϵͳ�ں������磺 386��486��586��x86
        System.out.println("OS.getArch() = " + OS.getArch());
        System.out.println("OS.getCpuEndian() = " + OS.getCpuEndian());//
        System.out.println("OS.getDataModel() = " + OS.getDataModel());//
        // ϵͳ����
        System.out.println("OS.getDescription() = " + OS.getDescription());
        System.out.println("OS.getMachine() = " + OS.getMachine());//
        // ����ϵͳ����
        System.out.println("OS.getName() = " + OS.getName());
        System.out.println("OS.getPatchLevel() = " + OS.getPatchLevel());//
        // ����ϵͳ������
        System.out.println("OS.getVendor() = " + OS.getVendor());
        // ��������
        System.out
                .println("OS.getVendorCodeName() = " + OS.getVendorCodeName());
        // ����ϵͳ����
        System.out.println("OS.getVendorName() = " + OS.getVendorName());
        // ����ϵͳ��������
        System.out.println("OS.getVendorVersion() = " + OS.getVendorVersion());
        // ����ϵͳ�İ汾��
        System.out.println("OS.getVersion() = " + OS.getVersion());
    }

    // c)ȡ��ǰϵͳ���̱��е��û���Ϣ
    public void testWho() {
        try {
            Sigar sigar = new Sigar();
            org.hyperic.sigar.Who[] who = sigar.getWhoList();
            if (who != null && who.length > 0) {
                for (int i = 0; i < who.length; i++) {
                    System.out.println("\n~~~~~~~~~" + String.valueOf(i)
                            + "~~~~~~~~~");
                    org.hyperic.sigar.Who _who = who[i];
                    System.out.println("��ȡ�豸getDevice() = " + _who.getDevice());
                    System.out.println("�������getHost() = " + _who.getHost());
                    System.out.println("��ȡ��ʱ��getTime() = " + _who.getTime());
                    // ��ǰϵͳ���̱��е��û���
                    System.out.println("��ȡ�û�getUser() = " + _who.getUser());
                }
            }
        } catch (SigarException e) {
            e.printStackTrace();
        }
    }

    // 4.��Դ��Ϣ����Ҫ��Ӳ�̣�
    // a)ȡӲ�����еķ���������ϸ��Ϣ��ͨ��sigar.getFileSystemList()�����FileSystem�б����Ȼ�������б�������
    public void testFileSystemInfo() throws Exception {
        Sigar sigar = new Sigar();
        FileSystem fslist[] = sigar.getFileSystemList();
        DecimalFormat df = new DecimalFormat("#0.00");
        // String dir = System.getProperty("user.home");// ��ǰ�û��ļ���·��
        for (int i = 0; i < fslist.length; i++) {
            System.out.println("\n~~~~~~~~~~" + i + "~~~~~~~~~~");
            FileSystem fs = fslist[i];
            // �������̷�����
            System.out.println("fs.getDevName() = " + fs.getDevName());
            // �������̷�����
            System.out.println("fs.getDirName() = " + fs.getDirName());
            System.out.println("fs.getFlags() = " + fs.getFlags());//
            // �ļ�ϵͳ���ͣ����� FAT32��NTFS
            System.out.println("fs.getSysTypeName() = " + fs.getSysTypeName());
            // �ļ�ϵͳ�����������籾��Ӳ�̡������������ļ�ϵͳ��
            System.out.println("fs.getTypeName() = " + fs.getTypeName());
            // �ļ�ϵͳ����
            System.out.println("fs.getType() = " + fs.getType());
            FileSystemUsage usage = null;
            try {
                usage = sigar.getFileSystemUsage(fs.getDirName());
            } catch (SigarException e) {
                if (fs.getType() == 2)
                    throw e;
                continue;
            }
            switch (fs.getType()) {
            case 0: // TYPE_UNKNOWN ��δ֪
                break;
            case 1: // TYPE_NONE
                break;
            case 2: // TYPE_LOCAL_DISK : ����Ӳ��
                // �ļ�ϵͳ�ܴ�С
                System.out.println(" Total = " + df.format((float)usage.getTotal()/1024/1024) + "G");
                // �ļ�ϵͳʣ���С
                System.out.println(" Free = " + df.format((float)usage.getFree()/1024/1024) + "G");
                // �ļ�ϵͳ���ô�С
                System.out.println(" Avail = " + df.format((float)usage.getAvail()/1024/1024) + "G");
                // �ļ�ϵͳ�Ѿ�ʹ����
                System.out.println(" Used = " + df.format((float)usage.getUsed()/1024/1024) + "G");
                double usePercent = usage.getUsePercent() * 100D;
                // �ļ�ϵͳ��Դ��������
                System.out.println(" Usage = " + df.format(usePercent) + "%");
                break;
            case 3:// TYPE_NETWORK ������
                break;
            case 4:// TYPE_RAM_DISK ������
                break;
            case 5:// TYPE_CDROM ������
                break;
            case 6:// TYPE_SWAP ��ҳ�潻��
                break;
            }
            System.out.println(" DiskReads = " + usage.getDiskReads());
            System.out.println(" DiskWrites = " + usage.getDiskWrites());
        }
        return;
    }

    // 5.������Ϣ
    // a)��ǰ��������ʽ����
    public String getFQDN() {
        Sigar sigar = null;
        try {
            return InetAddress.getLocalHost().getCanonicalHostName();
        } catch (UnknownHostException e) {
            try {
                sigar = new Sigar();
                return sigar.getFQDN();
            } catch (SigarException ex) {
                return null;
            } finally {
                sigar.close();
            }
        }
    }

    // b)ȡ����ǰ������IP��ַ
    public String getDefaultIpAddress() {
        String address = null;
        try {
            address = InetAddress.getLocalHost().getHostAddress();
            // û�г����쳣��������ȡ����IPʱ�����ȡ���Ĳ�������ѭ�ص�ַʱ�ͷ���
            // ������ͨ��Sigar���߰��еķ�������ȡ
            if (!NetFlags.LOOPBACK_ADDRESS.equals(address)) {
                return address;
            }
        } catch (UnknownHostException e) {
            // hostname not in DNS or /etc/hosts
        }
        Sigar sigar = new Sigar();
        try {
            address = sigar.getNetInterfaceConfig().getAddress();
        } catch (SigarException e) {
            address = NetFlags.LOOPBACK_ADDRESS;
        } finally {
            sigar.close();
        }
        return address;
    }

    // c)ȡ����ǰ������MAC��ַ
    public String getMAC() {
        Sigar sigar = null;
        try {
            sigar = new Sigar();
            String[] ifaces = sigar.getNetInterfaceList();
            String hwaddr = null;
            for (int i = 0; i < ifaces.length; i++) {
                NetInterfaceConfig cfg = sigar.getNetInterfaceConfig(ifaces[i]);
                if (NetFlags.LOOPBACK_ADDRESS.equals(cfg.getAddress())
                        || (cfg.getFlags() & NetFlags.IFF_LOOPBACK) != 0
                        || NetFlags.NULL_HWADDR.equals(cfg.getHwaddr())) {
                    continue;
                }
                /*
                 * ������ڶ������������������������Ĭ��ֻȡ��һ��������MAC��ַ�����Ҫ�������е���������������ĺ�����ģ�������޸ķ����ķ�������Ϊ�����Collection
                 * ��ͨ����forѭ����ȡ���Ķ��MAC��ַ��
                 */
                hwaddr = cfg.getHwaddr();
                break;
            }
            return hwaddr != null ? hwaddr : null;
        } catch (Exception e) {
            return null;
        } finally {
            if (sigar != null)
                sigar.close();
        }
    }

    // d)��ȡ������������Ϣ
    public void testNetIfList() throws Exception {
        Sigar sigar = new Sigar();
        String ifNames[] = sigar.getNetInterfaceList();
        for (int i = 0; i < ifNames.length; i++) {
            String name = ifNames[i];
            NetInterfaceConfig ifconfig = sigar.getNetInterfaceConfig(name);
            print("\nname = " + name);// �����豸��
            print("Address = " + ifconfig.getAddress());// IP��ַ
            print("Netmask = " + ifconfig.getNetmask());// ��������
            if ((ifconfig.getFlags() & 1L) <= 0L) {
                print("!IFF_UP...skipping getNetInterfaceStat");
                continue;
            }
            try {
                NetInterfaceStat ifstat = sigar.getNetInterfaceStat(name);
                print("RxPackets = " + ifstat.getRxPackets());// ���յ��ܰ�����
                print("TxPackets = " + ifstat.getTxPackets());// ���͵��ܰ�����
                print("RxBytes = " + ifstat.getRxBytes());// ���յ������ֽ���
                print("TxBytes = " + ifstat.getTxBytes());// ���͵����ֽ���
                print("RxErrors = " + ifstat.getRxErrors());// ���յ��Ĵ������
                print("TxErrors = " + ifstat.getTxErrors());// �������ݰ�ʱ�Ĵ�����
                print("RxDropped = " + ifstat.getRxDropped());// ����ʱ�����İ���
                print("TxDropped = " + ifstat.getTxDropped());// ����ʱ�����İ���
            } catch (SigarNotImplementedException e) {
            } catch (SigarException e) {
                print(e.getMessage());
            }
        }
    }

    void print(String msg) {
        System.out.println(msg);
    }

    // e)һЩ��������Ϣ
    public void getEthernetInfo() {
        Sigar sigar = null;
        try {
            sigar = new Sigar();
            String[] ifaces = sigar.getNetInterfaceList();
            for (int i = 0; i < ifaces.length; i++) {
                NetInterfaceConfig cfg = sigar.getNetInterfaceConfig(ifaces[i]);
                if (NetFlags.LOOPBACK_ADDRESS.equals(cfg.getAddress())
                        || (cfg.getFlags() & NetFlags.IFF_LOOPBACK) != 0
                        || NetFlags.NULL_HWADDR.equals(cfg.getHwaddr())) {
                    continue;
                }
                System.out.println("cfg.getAddress() = " + cfg.getAddress());// IP��ַ
                System.out
                        .println("cfg.getBroadcast() = " + cfg.getBroadcast());// ���ع㲥��ַ
                System.out.println("cfg.getHwaddr() = " + cfg.getHwaddr());// ����MAC��ַ
                System.out.println("cfg.getNetmask() = " + cfg.getNetmask());// ��������
                System.out.println("cfg.getDescription() = "
                        + cfg.getDescription());// ����������Ϣ
                System.out.println("cfg.getType() = " + cfg.getType());//
                System.out.println("cfg.getDestination() = "
                        + cfg.getDestination());
                System.out.println("cfg.getFlags() = " + cfg.getFlags());//
                System.out.println("cfg.getMetric() = " + cfg.getMetric());
                System.out.println("cfg.getMtu() = " + cfg.getMtu());
                System.out.println("cfg.getName() = " + cfg.getName());
                System.out.println();
            }
        } catch (Exception e) {
            System.out.println("Error while creating GUID" + e);
        } finally {
            if (sigar != null)
                sigar.close();
        }
    }

}