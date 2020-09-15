package com.atguigu.LogParse;

import com.atguigu.Log.LogDriver;
import com.atguigu.Log.LogMapper;
import com.atguigu.mapJoin.DistributedCacheDriver;
import com.atguigu.mapJoin.DistributedCacheMapper;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.net.URI;

/**
 * @Author 贾
 * @Date 2020/8/2917:57
 */
public class LogParseDriver {

    public static void main(String[] args) throws Exception{
        //1.获取job
        Configuration configuration = new Configuration();
        Job job = Job.getInstance(configuration);
        //2.设置路径
        job.setJarByClass(LogParseDriver.class);
        // 关联mapper
        job.setMapperClass(LogParseMapper.class);

        //关联map输出key,value
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);
        //map端的join逻辑不需要reduce阶段，因此将reduceTask的数量设置为0
        job.setNumReduceTasks(0);
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
