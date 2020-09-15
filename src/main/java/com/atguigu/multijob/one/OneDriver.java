package com.atguigu.multijob.one;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * @Author 贾
 * @Date 2020/8/3121:20
 *
 * 讲一个文件处理成倒排索引的格式，即：
 *  a.txt
 *      atguigu pingping
 *      atguigu
 *      atguigu pingping
 *  b.txt
 *      atguigu
 *      atguiu pingping
 *      heima heheh
 *  c.txt
 *      atguigu heima
 *      hehe
 *
 * 最终处理成文件如下：
 *  result.txt
 *      atguigu  a.txt-->3  b.txt-->2   c.txt--> 1
 *      pingping    a.txt-->2   b.txt-->1
 *      heima   b.txt-->1   c.txt-->1
 *      hehe    b.txt-->1   c.txt-->1
 *
 * 所以分成两个job处理
 *  第一个job：
 *      将结果处理成如下格式
 *      atguigu--a.txt  3
 *      atguigu--b.txt  2
 *      atguigu--c.txt  1
 *      pingping--a.txt 2
 *      pingping--b.txt 1
 *      ......
 *
 *  第二个job :
 *      将结果处理成正确的结果
 *          atguigu  a.txt-->3  b.txt-->2   c.txt--> 1
 *  *       pingping    a.txt-->2   b.txt-->1
 *  *       heima   b.txt-->1   c.txt-->1
 *          hehe    b.txt-->1   c.txt-->1
 *
 *
 *
 *
 *
 *
 */
public class OneDriver {

    public static void main(String[] args) throws Exception {

        //1.获取job
        Configuration configuration = new Configuration();

        Job job = Job.getInstance(configuration);
        //2.设置路径
        job.setJarByClass(OneDriver.class);
        // 关联mapper
        job.setMapperClass(OneMapper.class);
        //关联reduce
        job.setReducerClass(OneReduce.class);
        //关联map输出key,value
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        //关联最终输出keyValue
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

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
