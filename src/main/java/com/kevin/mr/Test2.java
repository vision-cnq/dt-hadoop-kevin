
package com.kevin.mr;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;

public class Test2 extends Configured implements Tool {


	enum Counter
	{
		LINESKIP,	//出错的行
	}
	
	/**  
	 * MAP任务
	 */  
	public static class Map extends Mapper<LongWritable, Text, Text, Text>
	{
		public void map ( LongWritable key, Text value, Mapper.Context context ) throws IOException, InterruptedException
		{
			String line = value.toString();				//读取源数据
			
			try
			{
				//数据处理
				String [] lineSplit = line.split(" ");
				String anum = lineSplit[0];
				String bnum = lineSplit[1];
				
				context.write( new Text(bnum), new Text(anum) );	//输出 key \t value
			}
			catch ( ArrayIndexOutOfBoundsException e )
			{
				context.getCounter(Counter.LINESKIP).increment(1);	//出错计数器+1
				return;
			}
		}
	}

	/**  
	 * REDUCE任务
	 */ 
	public static class Reduce extends Reducer<Text, Text, Text, Text>
	{
		public void reduce ( Text key, Iterable<Text> values, Context context ) throws IOException, InterruptedException
		{
			String valueString;
			String out = "";
			
			for ( Text value : values )
			{
				valueString = value.toString();
				out += valueString + "|";
			}
			
			context.write( key, new Text(out) );
		}
	}

	@Override
	public int run(String[] args) throws Exception 
	{
		Configuration conf = getConf();

		Job job = Job.getInstance(conf, "Test_2");								//任务名
		job.setJarByClass(Test2.class);								//指定class

		// 配置输入输出源
		String[] newArgs = new String[]{
				"hdfs://master:9000/test/input_02/",
				"hdfs://master:9000/test/output/" };

		SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddHHmmss");
		String date = sdf.format(new Date());

		// 指定job处理的数据路径
		FileInputFormat.setInputPaths(job, new Path(newArgs[0]));
		// 指定job处理数据输出结果的路径
		FileOutputFormat.setOutputPath(job, new Path(newArgs[1]+date));

		
		job.setMapperClass( Map.class );								//调用上面的Map类作为Map任务代码
		job.setReducerClass ( Reduce.class );							//调用上面的Reduce类作为Map任务代码
		job.setOutputFormatClass( TextOutputFormat.class );
		job.setOutputKeyClass( Text.class );							//指定输出的Key的格式
		job.setOutputValueClass( Text.class );							//指定输出的Value的格式
		
		job.waitForCompletion(true);
		
		//输出任务完成情况
		System.out.println( "任务名称：" + job.getJobName() );
		System.out.println( "任务成功：" + ( job.isSuccessful()?"是":"否" ) );
		System.out.println( "输入行数：" + job.getCounters().findCounter("org.apache.hadoop.mapred.Task$Counter", "MAP_INPUT_RECORDS").getValue() );
		System.out.println( "输出行数：" + job.getCounters().findCounter("org.apache.hadoop.mapred.Task$Counter", "MAP_OUTPUT_RECORDS").getValue() );
		System.out.println( "跳过的行数：" + job.getCounters().findCounter(Counter.LINESKIP).getValue() );

		return job.isSuccessful() ? 0 : 1;
	}
	

	public static void main(String[] args) throws Exception 
	{
		
		if ( args.length != 2 )
		{
			System.err.println("");
			System.err.println("Usage: Test_2 < input path > < output path > ");
			System.err.println("Example: hadoop jar ~/Test_2.jar hdfs://localhost:9000/home/james/Test_2 hdfs://localhost:9000/home/james/output");
			System.err.println("Counter:");
			System.err.println("\t"+"LINESKIP"+"\t"+"Lines which are too short");
			System.exit(-1);
		}

		SimpleDateFormat formatter = new SimpleDateFormat( "yyyy-MM-dd HH:mm:ss" );
		Date start = new Date();
		
		//运行任务
		int res = ToolRunner.run(new Configuration(), new Test2(), args);

		Date end = new Date();
		float time =  (float) (( end.getTime() - start.getTime() ) / 60000.0) ;
		System.out.println( "开始时间：" + formatter.format(start) );
		System.out.println( "结束时间：" + formatter.format(end) );
		System.out.println( "执行耗时：" + String.valueOf( time ) );

        System.exit(res);
	}
}