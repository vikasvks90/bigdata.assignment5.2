/**
 * <h1>Task6Reducer</h1>
 * Reducer program calculate the total units sold in each state for Onida company
 * This class will take input as (Key,Value) pair from output of mapper class
 * value will be a combined list for all the values for a given key
 * */
package mapreduce.assignment5.task6;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class Task6Reducer extends Reducer<Text, IntWritable, Text, IntWritable>{
	IntWritable outValue = new IntWritable();
	
	public void reduce(Text key, Iterable<IntWritable> values,Context context) throws IOException, InterruptedException{
		//we are taking the mapper class output and adding up all the values
		//to get the total units sold in each state for "ONIDA" company
		int sum = 0;
		for (IntWritable value : values) {
			sum += value.get();
		}
		
		outValue.set(sum);
		context.write(key, outValue);
	}
}