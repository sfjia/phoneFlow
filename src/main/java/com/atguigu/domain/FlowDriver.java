package com.atguigu.domain;

        import org.apache.hadoop.conf.Configuration;
        import org.apache.hadoop.fs.Path;
        import org.apache.hadoop.io.Text;
        import org.apache.hadoop.mapreduce.Job;
        import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
        import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * @Author 贾
 * @Date 2020/8/2419:13
 */
public class FlowDriver {

    public static void main(String[] args) throws Exception {
        //1.获取job
        Configuration configuration = new Configuration();
        Job job = Job.getInstance(configuration);
        //2.设置路径
        job.setJarByClass(FlowDriver.class);
        // 关联mapper
        job.setMapperClass(FlowMapper.class);
        //关联reduce
        job.setReducerClass(FlowReduce.class);
        //关联map输出key,value
        job.setMapOutputValueClass(FlowBean.class);
        job.setMapOutputKeyClass(Text.class);
        //关联最终输出keyValue
        job.setOutputValueClass(FlowBean.class);
        job.setOutputKeyClass(Text.class);
        //设置输入路径
        FileInputFormat.setInputPaths(job,new Path(args[0]));
        FileOutputFormat.setOutputPath(job,new Path(args[1]));
        //提交任务
        boolean b = job.waitForCompletion(true);
        System.exit(b ? 0 : 1);
    }
}
