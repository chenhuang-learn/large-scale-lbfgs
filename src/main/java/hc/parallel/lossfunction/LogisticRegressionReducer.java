package hc.parallel.lossfunction;

import hc.parallel.avro.entry;
import hc.parallel.util.FileOperator;

import java.io.IOException;

import org.apache.avro.mapred.AvroKey;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

public class LogisticRegressionReducer extends Reducer<IntWritable, FloatWritable, AvroKey<entry>, NullWritable>{
	
	private float[] weight = null;
	private float l2_c = (float) 0.0;
	
	@Override
	public void setup(Context context) throws IOException {
		// get lbfgs_data_max_index and lbfgs_l2_c
		l2_c = context.getConfiguration().getFloat("lbfgs_l2_c", (float) 1.0);
		int max_index = context.getConfiguration().getInt("lbfgs_data_max_index", -2);
		weight = new float[max_index + 1];
		// read weightFile in distributed cache
		FileOperator.readArrayLocal(new Path("weightFile"), context.getConfiguration(), weight);
	}
	
	// loss = add( w[i] * log(1+exp(-y[i]* weightT * x[i])) ) + 0.5 * lbfgs_l2_c * ||weight||2
	@Override
	public void reduce(IntWritable key, Iterable<FloatWritable> values, Context context) throws IOException, InterruptedException {
		if(key.get() == -1) {
			double loss = 0.0;
			for(FloatWritable f : values) {
				loss += f.get();
			}
			for(int i=0; i<weight.length; i++) {
				loss += 0.5 * l2_c * weight[i] * weight[i];
			}
			entry e = new entry(key.get(), (float)loss);
			context.write(new AvroKey<entry>(e), NullWritable.get());
		} else {
			double g = 0.0;
			for(FloatWritable f : values) {
				g += f.get();
			}
			g += l2_c * weight[key.get()];
			entry e = new entry(key.get(), (float)g);
			context.write(new AvroKey<entry>(e), NullWritable.get());
		}
	}
	
}
