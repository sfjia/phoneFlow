package com.atguigu.multijob.two;

import com.atguigu.multijob.one.OneDriver;
import com.atguigu.multijob.one.OneMapper;
import com.atguigu.multijob.one.OneReduce;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * @Author 贾
 * @Date 2020/8/3121:33
 */
public class TwoDriver {

    public static void main(String[] args) throws Exception {

        //1.获取job
        Configuration configuration = new Configuration();

        Job job = Job.getInstance(configuration);
        //2.设置路径
        job.setJarByClass(TwoDriver.class);
        // 关联mapper
        job.setMapperClass(TwoMapper.class);
        //关联reduce
        job.setReducerClass(TwoReduce.class);
        //关联map输出key,value
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        //关联最终输出keyValue
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

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
