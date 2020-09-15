package com.atguigu.reduceJoin;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Mapper;


import java.io.IOException;

/**
 * @Author 贾
 * @Date 2020/8/2910:44
 */
public class TableMapper extends Mapper<LongWritable, Text,Text,TableBean> {
    TableBean tableBean = new TableBean();
    Text k = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        //获取文件名
        FileSplit inputSplit = (FileSplit)context.getInputSplit();
        String fileName = inputSplit.getPath().getName();
        String line = value.toString();
        String[] fileds = line.split("\t");
        if(fileName.startsWith("order")){
            tableBean.setFlag("0");
            tableBean.setOrderId(fileds[0]);
            tableBean.setPid(fileds[1]);
            tableBean.setAmount(Integer.parseInt(fileds[2]));
            tableBean.setpName("");
            k.set(fileds[1]);
            context.write(k,tableBean);
        }else {
            tableBean.setOrderId("");
            tableBean.setPid(fileds[0]);
            tableBean.setpName(fileds[1]);
            tableBean.setAmount(0);
            tableBean.setFlag("1");
            k.set(fileds[0]);
            context.write(k,tableBean);
        }
    }
}
