/**
 * <h1>Task6Mapper</h1>
 * Mapper program calculate the total units sold in each state for Onida company
 * This class will take input as (Key,Value) pair from a given file and will
 * produce the output as (Key,Value) pair.
 * */
package mapreduce.assignment5.task6;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*; 

public class Task6Mapper extends Mapper<LongWritable, Text, Text, IntWritable> {
	Text outKey = new Text();
	IntWritable outValue = new IntWritable();
	
	public void map(LongWritable key, Text value, Context context) 
			throws IOException, InterruptedException {
		//every line will be split based on '|' and will be stored in String array
		String[] lineArray = value.toString().split("\\|");
		//if the company name is "ONIDA" put as mapper output key separated by state name
		if(lineArray[0].equalsIgnoreCase("ONIDA")) {
			outKey.set("ONIDA" + "\t" + lineArray[3]);
			outValue.set(1);
			context.write(outKey, outValue);
		}
	}
}
