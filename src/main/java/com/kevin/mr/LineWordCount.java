package com.kevin.mr;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * @author kevin
 * @version 1.0
 * @description     单词计数，按照根据空格分割。
 * @createDate 2018/12/17
 */
public class LineWordCount {

    public class WCMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

        // MapReduce框架每读一行数据就调用一次map方法
        @Override
        protected void map(LongWritable k1, Text v1,
                           Mapper<LongWritable, Text, Text, IntWritable>.Context context)
                throws IOException, InterruptedException {
            // 将这一行的内容转换成string类型
            String line = v1.toString();
            // 对这一行的文本按特定分隔符切分
            // String[] words = line.split(" ");
            String[] words = StringUtils.split(line, " ");
            // 遍历这个单词数组,输出为key-value形式 key：单词 value ： 1
            for (String word : words) {
                context.write(new Text(word), new IntWritable(1));
            }

        }

    }

    //经过mapper处理后的数据会被reducer拉取过来，所以reducer的KEYIN、VALUEIN和mapper的KEYOUT、VALUEOUT一致
    //经过reducer处理后的数据格式为<单词，频数>,所以KEYOUT为Text，VALUEOUT为IntWritable
    public class WCReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

        // 当mapper框架将相同的key的数据处理完成后，reducer框架会将mapper框架输出的数据<key,value>变成<key,values{}>。
        // 例如，在wordcount中会将mapper框架输出的所有<hello,1>变为<hello,{1,1,1...}>，即这里的<k2，v2s>，然后将<k2，v2s>作为reduce函数的输入
        @Override
        protected void reduce(Text k2, Iterable<IntWritable> v2s,
                              Reducer<Text, IntWritable, Text, IntWritable>.Context context)
                throws IOException, InterruptedException {
            int count = 0;
            // 遍历v2的list，进行累加求和
            for (IntWritable v2 : v2s) {
                count = v2.get();
            }
            // 输出这一个单词的统计结果
            context.write(k2, new IntWritable(count));
        }

    }


    /**
     *     用来描述一个特定的作业 比如:该作业使用哪个类作为逻辑处理中的map，哪个作为reduce
     *     还可以指定该作业要处理的数据所在的路径还可以指定改作业输出的结果放到哪个路径
     * @param args
     * @throws ClassNotFoundException
     * @throws InterruptedException
     * @throws IOException
     */
    public static void main(String[] args) throws ClassNotFoundException,
            InterruptedException, IOException {

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);

        // 配置输入输出源
        String[] newArgs = new String[]{
                "hdfs://master:9000/user/exe_mapreduce/wordcount/input",
                "hdfs://master:9000/user/exe_mapreduce/wordcount/output" };

        // 设置job所在的类在哪个jar包
        job.setJarByClass(LineWordCount.class);

        // 指定job所用的mappe类和reducer类
        job.setMapperClass(WCMapper.class);
        job.setReducerClass(WCReducer.class);

        // 指定mapper输出类型和reducer输出类型
        // 由于在wordcount中mapper和reducer的输出类型一致，
        // 所以使用setOutputKeyClass和setOutputValueClass方法可以同时设定mapper和reducer的输出类型
        // 如果mapper和reducer的输出类型不一致时，可以使用setMapOutputKeyClass和setMapOutputValueClass单独设置mapper的输出类型
        // job.setMapOutputKeyClass(Text.class);
        // job.setMapOutputValueClass(IntWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        // 检查输出路径是否存在，存在则删除
        /*Path in = new Path(newArgs[0]);
        Path out = new Path(newArgs[1]);
        try {
            FileSystem fs = FileSystem.get(new URI(in.toString()), new Configuration());
            // 如果hdfs中存在输出目录则删除
            if(fs.exists(out)){
                fs.delete(out,true);
            }
        } catch (URISyntaxException e) {
            e.printStackTrace();
        }*/

        SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddHHmmss");
        String date = sdf.format(new Date());

        // 指定job处理的数据路径
        FileInputFormat.setInputPaths(job, new Path(newArgs[0]));
        // 指定job处理数据输出结果的路径
        FileOutputFormat.setOutputPath(job, new Path(newArgs[1]+date));

        // 将job提交给集群运行
        job.waitForCompletion(true);
    }

}
