package mr.kpi;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


//Tool:处理命令行参数的接口。Tool是MR的任何tool/app的标准。这些实现应该代理对标准命令行参数的处理。
//Configured：Base class for things that may be configured with a Configuration.
public class KPIIP extends Configured implements Tool {
	public static class MikeMapper extends Mapper<LongWritable,Text,Text,Text> {
		@Override
		protected void setup(Context context)
				throws IOException, InterruptedException {
			super.setup(context);
		}		
		private Text request = new Text();
		private Text addr = new Text();
		@Override
		protected void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			KPI kpi = KPI.parser(value.toString());
			if(kpi.isValid()) {
				request.set(kpi.getRequest());
				addr.set(kpi.getRemote_addr());
				context.write(request, addr);
			}
		}
		@Override
		protected void cleanup(Context context)
				throws IOException, InterruptedException {
			super.cleanup(context);
		}
	}
	
	public static class MikeReducer extends Reducer<Text,Text,Text,IntWritable> {
		
		//OutputCollector和Reporter是Hadoop-0.19以前版本里面的API，在Hadoop-0.20.2以后就换成Context，Context的功能包含了OutputCollector和Reporter的功能，此外还添加了一些别的功能。
		@Override
		protected void setup(Context context)
				throws IOException, InterruptedException {
			super.setup(context);
		}
		private IntWritable result = new IntWritable();
		@Override
		protected void reduce(Text key, Iterable<Text> values,
				Context context) throws IOException, InterruptedException {
			Set<Text> count = new HashSet<Text>();
			int sum = 0;
			for(Text value:values) {
//				System.out.println(value);    //每次输出的值都是1
				count.add(value);
			}
			result.set(count.size());
			context.write(key, result);
		}

		@Override
		protected void cleanup(Context context)
				throws IOException, InterruptedException {
			super.cleanup(context);
		}
		
	}
	
	@Override
	public int run(String[] args) throws Exception {
		String input = "hdfs://10.1.9.70:9000/inputLog/access.log";
		String output = "hdfs://10.1.9.70:9000/outputIPLog";
		//1 create job
		Job	job = Job.getInstance(this.getConf(), this.getClass().getSimpleName());
		//set job run class
		job.setJarByClass(KPIIP.class);
		
		//2 set job
		//step 1: set inputFormat
		job.setInputFormatClass(TextInputFormat.class);
	    //step 2: set inputPathThe method setOutputFormatClass(Class<? extends OutputFormat>) in the type Job is not applicable for the arg
		Path inputPath = new Path(input);
		FileInputFormat.addInputPath(job, inputPath);
		//step 3: set mapper class
		job.setMapperClass(MikeMapper.class);
		//step 4: set mapoutput key/value class
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		//step 5: set shuffle(sort,combiner,group)
		//set sort
		job.setSortComparatorClass(Text.Comparator.class);
		//set combiner(option, default is unset),Reducer subClass
		//job.setCombinerClass(null);
		//set group
		job.setGroupingComparatorClass(Text.Comparator.class);
		//step 6: set Reduce class
		job.setReducerClass(MikeReducer.class);
		//step 7: job output key/value class
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		//step 8: set outputFormat
		job.setOutputFormatClass(TextOutputFormat.class);
		//step 9: set output path
		Path outputPath = new Path(output);
		FileOutputFormat.setOutputPath(job, outputPath);
		//3 submit job
		boolean isComplete = job.waitForCompletion(true);
		return isComplete ? 0 : 1;
	}

	public static void main(String[] args) throws Exception {
		int status = ToolRunner.run(new KPIIP(), args);
		System.exit(status);
	}
}
