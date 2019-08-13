package org.apache.hadoop.dfs;

import java.util.Date;

import org.apache.hadoop.io.UTF8;

/**
 * 关于数据节点状态的报告信息。
 * @author 章云
 * @date 2019/8/8 21:02
 */
public class DataNodeReport {
    private String name;
    private String host;
    private long capacity;
    private long remaining;
    private long lastUpdate;

    /**
     * 数据节点hostname
     */
    public String getName() {
        return name;
    }

    /**
     * 数据节点hostname
     */
    public String getHost() {
        return host;
    }

    /**
     * 总容量
     */
    public long getCapacity() {
        return capacity;
    }

    /**
     * 可用容量
     */
    public long getRemaining() {
        return remaining;
    }

    /**
     * 这个信息准确的时间。
     */
    public long getLastUpdate() {
        return lastUpdate;
    }

    public void setName(String name) {
        this.name = name;
    }

    public void setHost(String host) {
        this.host = host;
    }

    public void setCapacity(long capacity) {
        this.capacity = capacity;
    }

    public void setRemaining(long remaining) {
        this.remaining = remaining;
    }

    public void setLastUpdate(long lastUpdate) {
        this.lastUpdate = lastUpdate;
    }

    @Override
    public String toString() {
        StringBuffer buffer = new StringBuffer();
        long c = getCapacity();
        long r = getRemaining();
        long u = c - r;
        buffer.append("Name: " + name + "\n");
        buffer.append("Total raw bytes: " + c + " (" + DFSShell.byteDesc(c) + ")" + "\n");
        buffer.append("Used raw bytes: " + u + " (" + DFSShell.byteDesc(u) + ")" + "\n");
        buffer.append("% used: " + DFSShell.limitDecimal(((1.0 * u) / c) * 100, 2) + "%" + "\n");
        buffer.append("Last contact: " + new Date(lastUpdate) + "\n");
        return buffer.toString();
    }

}
