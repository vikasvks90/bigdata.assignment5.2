/**
 * <h1>Task7Reducer</h1>
 * Reducer program to calculate the total units sold for each Company
 * This class will take input as (Key,Value) pair from output of mapper class
 * value will be a combined list for all the values for a given key
 * */
package mapreduce.assignment5.task7;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

public class Task7Reducer extends Reducer<SortTV, IntWritable, SortTV, IntWritable>
{
	IntWritable outValue = new IntWritable();
	//input key to reducer is of SortTV type which will be in sorted order based on company name and tv size
	public void reduce(SortTV key, Iterable<IntWritable> values,Context context) throws IOException, InterruptedException
	{
		//we are taking the value of mapper class which is (1) and adding up all the values
		//to get the total units sold for each company
		int sum = 0;
		for (IntWritable value : values) {
			sum += value.get();
		}
		
		//output of reducer is total number of units sold for each company
		outValue.set(sum);
		context.write(key, outValue);
	}
}
