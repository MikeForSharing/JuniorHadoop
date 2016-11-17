package STJoin;
import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
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
public class STJoin extends Configured implements Tool {
	public static int time = 0;
	public static class MikeMapper extends Mapper<LongWritable,Text,Text,Text> {
		@Override
		public void setup(Context context)
				throws IOException, InterruptedException {
			super.setup(context);
		}		
		
		@Override
		protected void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			String parentName = new String();
			String childName = new String();
			String relationType = new String();
			String line = value.toString();
			int i = 0;
			
			while(line.charAt(i) != ' ') {
				i++;
			}
			
			String[] values = {line.substring(0,i),line.substring(i+1)};
			if(values[0].compareTo("child") != 0) {
				childName = values[0];
//System.out.println(childName);
				parentName = values[1];
//System.out.println(parentName);

				relationType = "1";
				context.write(new Text(values[1]), new Text(relationType + "+" + childName + "+" + parentName));
				relationType = "2";
				context.write(new Text(values[0]), new Text(relationType + "+" + childName + "+" + parentName));
			}
		}
		@Override
		protected void cleanup(Context context)
				throws IOException, InterruptedException {
			super.cleanup(context);
		}
	}
	
	public static class MikeReducer extends Reducer<Text,Text,Text,Text> {
		//OutputCollector和Reporter是Hadoop-0.19以前版本里面的API，在Hadoop-0.20.2以后就换成Context，Context的功能包含了OutputCollector和Reporter的功能，此外还添加了一些别的功能。
		@Override
		protected void setup(Context context)
				throws IOException, InterruptedException {
			super.setup(context);
		}
		
		@Override
		protected void reduce(Text key, Iterable<Text> values,
				Context context) throws IOException, InterruptedException {
			if(time == 0) {
				context.write(new Text("grandChild"), new Text("grandParent"));
				time++;
			} 
			
			int grandChildNum = 0;
			String[] grandChild = new String[15];
			int grandParentNum = 0;
			String[] grandParent = new String[15];
			Iterator ite = values.iterator();
			while(ite.hasNext()) {
				int i = 2;
				String record = ite.next().toString();
//System.out.println(record);

				int len = record.length();
				if(len == 0) {
					continue;
				}
				char relationType = record.charAt(0);
//System.out.println(relationType);
				String childName = new String();
				String parentName = new String();
				while(record.charAt(i) != '+') {
					childName += record.charAt(i);
					i++;
				}
//System.out.println(childName);

				
				i +=1;
				while(i<record.length()) {
					parentName += record.charAt(i);
					i++;
				}
//System.out.println(parentName);
			
				if(relationType == '1') {
					grandChild[grandChildNum] = childName;
//System.out.println(childName);
//System.out.println(grandChild[grandChildNum]);

					grandChildNum++;
//System.out.println(grandChild[grandChildNum]);
				}else {
					grandParent[grandParentNum] = parentName;
					grandParentNum++;
				}
			}
//System.out.println(grandChildNum);
//System.out.println(grandParentNum);

			//grandChild和grandParent求迪卡尔积
			if(grandChildNum != 0 && grandParentNum != 0) {
				for(int m=0;m<grandChildNum;m++) {
					for(int n=0;n<grandParentNum;n++) {
						context.write(new Text(grandChild[m]), new Text(grandParent[n]));
					}
				}
			}
			
		}

		@Override
		protected void cleanup(Context context)
				throws IOException, InterruptedException {
			super.cleanup(context);
		}
		
	}
	
	@Override
	public int run(String[] args) throws Exception {
		//1 create job
		Job	job = Job.getInstance(this.getConf(), this.getClass().getSimpleName());
		//set job run class
		job.setJarByClass(STJoin.class);
		
		//2 set job
		//step 1: set inputFormat
		job.setInputFormatClass(TextInputFormat.class);
	    //step 2: set inputPathThe method setOutputFormatClass(Class<? extends OutputFormat>) in the type Job is not applicable for the arg
		Path inputPath = new Path("hdfs://10.1.9.70:9000/input/grandparents.txt");
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
		job.setOutputValueClass(Text.class);
		//step 8: set outputFormat
		job.setOutputFormatClass(TextOutputFormat.class);
		//step 9: set output path
		Path outputPath = new Path("hdfs://10.1.9.70:9000/outputSTJoin");
		FileOutputFormat.setOutputPath(job, outputPath);
		//3 submit job
		boolean isComplete = job.waitForCompletion(true);
		return isComplete ? 0 : 1;
	}

	public static void main(String[] args) throws Exception {
		int status = ToolRunner.run(new STJoin(), args);
		System.exit(status);
	}
}
