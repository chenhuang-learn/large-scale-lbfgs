package parallel.lossfunction;

import java.io.IOException;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

public class LogisticRegressionLessMemoryCombiner extends Reducer<IntWritable, FloatWritable, IntWritable, FloatWritable> {
	@Override
	public void reduce(IntWritable key, Iterable<FloatWritable> values, Context context) throws IOException, InterruptedException {
		double sum = 0.0;
		for(FloatWritable f : values) {
			sum += f.get();
		}
		context.write(key, new FloatWritable((float) sum));
	}
}
