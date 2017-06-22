/**
 * <h1>Task7</h1>
 * This class will be used to define the configuration components for the given task
 * */
package mapreduce.assignment5.task7;

import org.apache.hadoop.fs.Path; 
import org.apache.hadoop.conf.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat; 
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat; 
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat; 
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.io.IntWritable;

public class Task7 {
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = new Job(conf, "Task7");
		job.setJarByClass(Task7.class);
		
		//map output key will use SortTV class
		job.setMapOutputKeyClass(SortTV.class);
		job.setMapOutputValueClass(IntWritable.class);
		
		//map output key will use SortTV class
		//to ensure inside every reducer, keys are sorted
		job.setOutputKeyClass(SortTV.class);
		job.setOutputValueClass(IntWritable.class);
		
		job.setMapperClass(Task7Mapper.class);
		job.setReducerClass(Task7Reducer.class);
		
		//it will set the number of reducer tasks equal to 4
		job.setNumReduceTasks(4);
		 
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.addInputPath(job, new Path(args[0])); 
		FileOutputFormat.setOutputPath(job,new Path(args[1]));
		
		job.waitForCompletion(true);
	}
}