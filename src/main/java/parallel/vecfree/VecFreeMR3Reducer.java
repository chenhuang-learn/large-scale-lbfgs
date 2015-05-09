package parallel.vecfree;

import java.io.IOException;

import org.apache.avro.mapred.AvroKey;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

import parallel.avro.entry;

public class VecFreeMR3Reducer extends Reducer<IntWritable, FloatWritable, AvroKey<entry>, NullWritable>{
	
	@Override
	public void reduce(IntWritable key, Iterable<FloatWritable> values, Context context) throws IOException, InterruptedException {
		double d = 0.0;
		for(FloatWritable f : values) {
			d += f.get();
		}
		entry e = new entry(key.get(), (float)d);
		context.write(new AvroKey<entry>(e), NullWritable.get());
	}
	
}
