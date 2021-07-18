package com.jike.mapreduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

;import java.io.IOException;

/**
 * 组装Job=Map+Reduce
 */
public class MainWritable {

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {


        //args = new String[]{"D:\\Download\\chrome\\MapReduce编程作业题及数据\\HTTP_20130313143750.dat","D:\\Download\\chrome\\MapReduce编程作业题及数据\\out\\HTTP_20210718143750.dat"};
        try{
            if(args.length!=2){
                //如果传递的参数不够，程序直接退出
                System.exit(100);
            }


        //1.获取job信息
        Configuration conf = new Configuration();

        Job job = Job.getInstance(conf);

        //2.加载jar包
        job.setJarByClass(MainWritable.class);

        //3.关联map和reduce
        job.setMapperClass(MapWritable.class);
        job.setReducerClass(ReduceWritable.class);

        //4.设置最终输出类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(FlowBean.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(FlowBean.class);


        //指定reduce task数量，跟ProvincePartitioner的分区数匹配
        job.setNumReduceTasks(1);     //Reduce的个数在这里设定


        //5.设置输入和输出路径
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        //6.提交job任务
        job.waitForCompletion(true);
        }catch(Exception e){
            e.printStackTrace();
        }


    }
}
