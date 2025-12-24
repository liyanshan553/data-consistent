package com.smee.binlog.collector;

import lombok.Getter;

@Getter
public class PerfMonitorContext {
    // getter 方法 (静态)
    @Getter
    private static double parseCostTime = 0;
    private static double convertCostTime = 0;
    private static double sendCostTime = 0;
    private static double totalCostTime = 0;
    private static double beginTime = 0;
    private static double endTime = 0;
    private static double beginWallTime = 0;
    private static double endWallTime = 0;
    public static boolean hasRecordedBegin = false;
    public static boolean hasRecordedEnd = false;
    public static boolean isFinishedCurrentFile = false;
    public static String lastFileName = null;
    public static String currentFileName = null;

    public static double getConvertCostTime() { return convertCostTime; }

    public static double getSendCostTime() { return sendCostTime; }

    public static double getBeginTime() { return beginTime; }

    public static double getBeginWallTime() { return beginWallTime; }

    public static double getEndTime() { return endTime; }

    public static double getEndWallTime() { return endWallTime; }

    // 注意：图片中这里拼写似乎是 getTatalCostTime，这里还原为 getTotalCostTime 以符合逻辑，或按原图
    public static double getTotalCostTime() { return totalCostTime; }

    // setter 方法 (静态)
    public static void setParseCostTime(double parseCostTime) { PerfMonitorContext.parseCostTime = parseCostTime; }

    public static void setConvertCostTime(double convertCostTime) {
        PerfMonitorContext.convertCostTime = convertCostTime;
    }

    public static void setSendCostTime(double sendCostTime) { PerfMonitorContext.sendCostTime = sendCostTime; }

    public static void setTotalCostTime(double totalCostTime) { PerfMonitorContext.totalCostTime = totalCostTime; }

    public static void setBeginTime(double beginTime) { PerfMonitorContext.beginTime = beginTime; }

    public static void setBeginWallTime(double beginWallTime) { PerfMonitorContext.beginWallTime = beginWallTime; }

    public static void setEndTime(double endTime) { PerfMonitorContext.endTime = endTime; }

    public static void setEndWallTime(double endWallTime) { PerfMonitorContext.endWallTime = endWallTime; }

    // 累加方法 (更常用)
    public static void addParseCostTime(double time) { parseCostTime += time; }

    public static void addConvertCostTime(double time) { convertCostTime += time; }

    public static void addSendCostTime(double time) { sendCostTime += time; }

    // 重置方法
    public static void reset() {
        parseCostTime = 0L;
        convertCostTime = 0L;
        sendCostTime = 0L;
    }

    // 获取总耗时 (图片逻辑似乎有两处 getTotalCostTime，底部这个是计算逻辑)
    // 注意：此处重载或覆盖了上面的 getter，逻辑是三者相加
    // public static double getTotalCostTime() { return parseCostTime + convertCostTime + sendCostTime; } 

    public static void resetForNewFile() {
        hasRecordedBegin = false;
        hasRecordedEnd = false;
        isFinishedCurrentFile = false;
        PerfMonitorContext.setParseCostTime(0L);
        PerfMonitorContext.setConvertCostTime(0L);
        PerfMonitorContext.setSendCostTime(0L);
        PerfMonitorContext.setTotalCostTime(0L);
    }
}