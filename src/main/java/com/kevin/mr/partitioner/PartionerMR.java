package com.kevin.mr.partitioner;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * @author kevin
 * @version 1.0
 * @description     使用分区
 * @createDate 2018/12/19
 */
public class PartionerMR {

    /**
     *  4个泛型中，前两个是指定的mapper输入数据的类型
     *  map 和 reduce 的数据输入输出是以key-value的形式封装的
     *  默认情况下，框架传递给我们的mapper的输入数据中，key是要处理的文本中一行的偏移量，这一行的内容作为value
     *  JDK 中long string等使用jdk自带的序列化机制，序列化之后会携带很多附加信息，造成网络传输冗余，
     *  所以Hadoop自己封装了一些序列化机制
     */
    public static class PartitionerMapper extends Mapper<LongWritable,Text,Text,LongWritable>{

        // mapreduce框架每读一行就调用一次该方法

        @Override
        protected void map(LongWritable key, Text value,Context context) throws IOException, InterruptedException {
            //具体的业务写在这个方法中，而且我们业务要处理的数据已经被该框架传递进来
            // key是这一行的偏移量，value是文本内容
            String line = value.toString();
            String[] words = StringUtils.split(line, " ");
            for(String word : words){
                context.write(new Text(word), new LongWritable(1));      //context实例用于输出内容的写入
            }
        }
    }

    /**
    *    框架在map处理完成之后，将所有的kv对缓存起来，进行分组，然后传递一个组
    *    <key,{value1,value2...valuen}>
    *    <hello,{1,1,1,1,1,1.....}>
     */
    public static class PartitionerReducer extends Reducer<Text,LongWritable,Text,LongWritable> {

        @Override
        protected void reduce(Text key, Iterable<LongWritable> values,Context context) throws IOException, InterruptedException {
            long count = 0;
            for(LongWritable value:values){
                count += value.get();
            }
            context.write(key, new LongWritable(count));
        }

    }

    // 做分区
    public static class WCPartitioner extends Partitioner<Text, LongWritable> {

        @Override
        public int getPartition(Text key, LongWritable value, int numOfPartitions) {
            return key.toString().length() % numOfPartitions;
        }
    }


    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();

        // 配置输入输出源
        String[] newArgs = new String[]{
                "hdfs://master:9000/test/word.txt",
                "hdfs://master:9000/test/output/" };

        Job job = Job.getInstance(conf);

        // 找到Mapper和Reducer两个类所在的路径
        //设置整个job所用的那些类在哪个jar下
        job.setJarByClass(PartionerMR.class);

        //本job使用的mapper和reducer类
        job.setMapperClass(PartitionerMapper.class);
        job.setReducerClass(PartitionerReducer.class);

        //指定reduce的输出数据kv类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);

        // 指定map的输出数据的kv类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(LongWritable.class);

        //指定自定义分区
        job.setPartitionerClass(WCPartitioner.class);

        //指定Reducer的输出的文件个数
        job.setNumReduceTasks(3);

        SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddHHmmss");
        String date = sdf.format(new Date());

        // 指定job处理的数据路径
        FileInputFormat.setInputPaths(job, new Path(newArgs[0]));
        // 指定job处理数据输出结果的路径
        FileOutputFormat.setOutputPath(job, new Path(newArgs[1]+date));

        job.waitForCompletion(true);


    }


}
