package hadoop;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class abc{
	
	public static class WordCountMapper extends Mapper<LongWritable,Text,Text,IntWritable>{
		
		public void map(LongWritable key,Text value,Context context ) throws IOException, InterruptedException {
			String val = value.toString();
			StringTokenizer st = new StringTokenizer(val);
			Text word = new Text();
			while(st.hasMoreTokens()) {
				word.set(st.nextToken());
				context.write(word, new IntWritable(1));
			}
		}
		
	}
	
	public static class WordCountReducer extends Reducer<Text,IntWritable,Text,IntWritable>{
		
		public void reduce(Text key,Iterable<IntWritable> value,Context context) throws IOException, InterruptedException {
			int sum = 0;
			IntWritable result = new IntWritable();
			for(IntWritable val : value) {
				sum = sum + val.get();
			}
			result.set(sum);
			context.write(key, result);
		}
	}
	
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		Configuration con = new Configuration();
		Job job = Job.getInstance(con, "WordCount");
		job.setJarByClass(abc.class);
		job.setMapperClass(WordCountMapper.class);
		job.setReducerClass(WordCountReducer.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		
		Path outputpath = new Path(args[1]);
		
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, outputpath);
		
		outputpath.getFileSystem(con).delete(outputpath, true);
		
		System.exit(job.waitForCompletion(true)?0:1 );
	}

}
