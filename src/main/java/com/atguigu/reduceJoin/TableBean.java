package com.atguigu.reduceJoin;


import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * @Author 贾
 * @Date 2020/8/2910:37
 */
public class TableBean implements Writable {

    private String orderId;
    private String pid;
    private int amount;
    private String pName;
    private String flag;

    public TableBean() {
    }

    @Override
    public String toString() {
        return  orderId + '\t' +
                pName + '\t' +
                amount ;

    }

    public String getOrderId() {
        return orderId;
    }

    public void setOrderId(String orderId) {
        this.orderId = orderId;
    }

    public String getPid() {
        return pid;
    }

    public void setPid(String pid) {
        this.pid = pid;
    }

    public int getAmount() {
        return amount;
    }

    public void setAmount(int amount) {
        this.amount = amount;
    }

    public String getpName() {
        return pName;
    }

    public void setpName(String pName) {
        this.pName = pName;
    }

    public String getFlag() {
        return flag;
    }

    public void setFlag(String flag) {
        this.flag = flag;
    }

    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(orderId);
        dataOutput.writeUTF(pid);
        dataOutput.writeUTF(pName);
        dataOutput.writeInt(amount);
        dataOutput.writeUTF(flag);
    }

    public void readFields(DataInput dataInput) throws IOException {
        orderId = dataInput.readUTF();
        pid = dataInput.readUTF();
        pName = dataInput.readUTF();
        amount = dataInput.readInt();
        flag = dataInput.readUTF();
    }
}
