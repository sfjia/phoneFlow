package com.atguigu.reduceJoin;

import com.atguigu.Log.LogDriver;
import com.atguigu.Log.LogMapper;
import com.atguigu.Log.LogOuputFormat;
import com.atguigu.Log.LogReduce;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * @Author 贾
 * @Date 2020/8/2911:21
 */
public class TableDriver {
    public static void main(String[] args)throws Exception{
        //1.获取job
        Configuration configuration = new Configuration();
        Job job = Job.getInstance(configuration);
        //2.设置路径
        job.setJarByClass(TableDriver.class);
        // 关联mapper
        job.setMapperClass(TableMapper.class);
        //关联reduce
        job.setReducerClass(TableReduce.class);
        //关联map输出key,value
        job.setMapOutputValueClass(TableBean.class);
        job.setMapOutputKeyClass(Text.class);
        //关联最终输出keyValue
        job.setOutputValueClass(TableBean.class);
        job.setOutputKeyClass(NullWritable.class);

      //  job.setOutputFormatClass(LogOuputFormat.class);
        //设置输入路径
        FileInputFormat.setInputPaths(job,new Path(args[0]));
        // 虽然我们自定义了outputformat，但是因为我们的outputformat继承自fileoutputformat
        // 而fileoutputformat要输出一个_SUCCESS文件，所以，在这还得指定一个输出目录
        FileOutputFormat.setOutputPath(job,new Path(args[1]));
        //提交任务
        boolean b = job.waitForCompletion(true);
        System.exit(b ? 0 : 1);
    }
}
