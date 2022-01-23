package com.atguigu.mapreduce.PartitionerandwritableComparable;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * 1、定义类实现writable接口
 * 2、重写序列化和反序列化方法
 * 3、重写空参构造
 * 4、toString方法
 */
public class FlowBean implements WritableComparable<FlowBean> {//实现Writable接口

    private long upFlow;
    private long downFlow;
    private long sumFlow;

    //反序列化时，需要反射调用空参构造函数，所以必须有空参构造
    public FlowBean() {
    }

    public long getUpFlow() {
        return upFlow;
    }

    public void setUpFlow(long upFlow) {
        this.upFlow = upFlow;
    }

    public long getDownFlow() {
        return downFlow;
    }

    public void setDownFlow(long downFlow) {
        this.downFlow = downFlow;
    }

    public long getSumFlow() {
        return sumFlow;
    }

    public void setSumFlow() {
        this.sumFlow = this.upFlow + this.downFlow;
    }

    //重写序列化方法
    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeLong(upFlow);
        dataOutput.writeLong(downFlow);
        dataOutput.writeLong(sumFlow);
    }

    //重写反序列方法
    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.upFlow = dataInput.readLong();
        this.downFlow = dataInput.readLong();
        this.sumFlow = dataInput.readLong();
    }
    //反序列化的顺序必须和序列化的顺序相同

    @Override
    public String toString() {
        return upFlow + "\t" + downFlow + "\t" + sumFlow;
    }

    @Override
    public int compareTo(FlowBean o) {
        if (this.sumFlow > o.sumFlow){
            return -1;
        }else if (this.sumFlow < o.sumFlow){
            return 1;
        }else{
            //按照上行流量的正序排
            if (this.upFlow > o.upFlow){
                return 1;
            }else if (this.upFlow < o.upFlow){
                return -1;
            }else{
                return 0;
            }

        }
    }
}
