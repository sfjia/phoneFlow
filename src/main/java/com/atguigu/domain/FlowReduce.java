package com.atguigu.domain;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @Author 贾
 * @Date 2020/8/2419:08
 */
public class FlowReduce extends Reducer<Text,FlowBean, Text,FlowBean> {

    FlowBean flowBean = new FlowBean();

    @Override
    protected void reduce(Text key, Iterable<FlowBean> values, Context context) throws IOException, InterruptedException {
        long sumUpflow=0;
        long sumdownflow=0;
        for (FlowBean bean : values) {
            sumUpflow += bean.getUpFlow();
            sumdownflow += bean.getDownFlow();
        }
        //2.封装
        flowBean.set(sumUpflow,sumdownflow);
        //3.写出去
        context.write(key,flowBean);
    }
}
