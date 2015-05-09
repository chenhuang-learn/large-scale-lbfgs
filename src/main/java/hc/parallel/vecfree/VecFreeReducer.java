package hc.parallel.vecfree;

import java.io.IOException;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

public class VecFreeReducer extends Reducer<IntPair, FloatWritable, IntWritable, FloatWritable> {
	
	private double result = 0.0;
	private int jobNum = -1;
	
	@Override
	public void setup(Context context) {
		jobNum = context.getTaskAttemptID().getTaskID().getId();
		System.out.println("jobNum : " + jobNum);
		result = 0.0;
	}
	
	@Override
	public void reduce(IntPair key, Iterable<FloatWritable> values, Context context) {
		if(jobNum != key.getJobNum()) {
			throw new IllegalStateException("jobNum_1, jobNum_2 : " + jobNum + ", " + key.getJobNum());
		}
		int num = 0;
		float value_1 = 0.f;
		float value_2 = 0.f;
		for(FloatWritable f : values) {
			num += 1;
			if(num == 1) {
				value_1 = f.get();
			} else if(num == 2) {
				value_2 = f.get();
			} else {
				throw new IllegalStateException("num of values in mr2 reduce : " + num);
			}
		}
		result += value_1 * value_2;
	}
	
	@Override
	public void cleanup(Context context) throws IOException, InterruptedException {
		context.write(new IntWritable(jobNum), new FloatWritable((float)result));
	}
	
}
