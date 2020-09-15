package com.atguigu.domain;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.yarn.logaggregation.AggregatedLogFormat;

import java.io.IOException;

/**
 * @Author 贾
 * @Date 2020/8/2418:56
 *
 *
 */
public class FlowMapper extends Mapper<LongWritable, Text,Text, FlowBean> {

    /**
     *
     * @param key
     * @param value
     * @param context
     * @throws IOException
     * @throws InterruptedException
     *
     * 一行的格式 第二个字段phone， 倒数第三个为上行流量  倒数第二个为下行流量 ：
     *  XXX     phone   XXX     XXX     XXX  ...   upflow   dowflow    XXXX
     *
     */

    FlowBean flowBean = new FlowBean();
    Text k = new Text();
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        //获取一行
        String line = value.toString();
        //  切割
        String[] fields = line.split("\t");

        String phone = fields[1];

        Long  upFlow = Long.parseLong(fields[fields.length - 3]);

        Long  downFlow = Long.parseLong(fields[fields.length - 2]);

        flowBean.set(upFlow,downFlow);
        k.set(phone);
        context.write(k,flowBean);

    }
}
