package hc.parallel.test;

import hc.parallel.avro.entry;
import hc.parallel.avro.lbfgsdata;
import hc.parallel.util.FileOperator;

import java.io.IOException;
import java.util.List;

import org.apache.avro.mapred.AvroKey;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Mapper;

public class MRTestMapper extends Mapper<AvroKey<lbfgsdata>, NullWritable, IntWritable, FloatWritable>{
	
	private float[] x = null;
	private int positiveNum = 0;
	private int negativeNum = 0;
	
	@Override
	public void setup(Context context) throws IOException {
		int max_index = context.getConfiguration().getInt("lbfgs_data_max_index", -2);
		x = new float[max_index + 1];
		FileOperator.readArrayLocal(new Path("weightFile"), context.getConfiguration(), x);
		positiveNum = 0;
		negativeNum = 0;
	}
	
	@Override
	public void map(AvroKey<lbfgsdata> key, NullWritable nullvalue, Context context) throws IOException, InterruptedException {
		lbfgsdata data = key.datum();
		
		float offset = data.getOffset();
		List<entry> l = data.getFeatures();
		int y = data.getResponse();
		
		double prob = 0.0;
		for (entry e : l) {
			prob += e.getValue() * x[e.getIndex()];
		}
		prob += offset;
		prob = 1 / (1 + Math.exp(-prob));
		
		if(y == -1) {
			context.write(new IntWritable(2), new FloatWritable((float)prob));
			negativeNum += 1;
		} else if(y == 1) {
			context.write(new IntWritable(3), new FloatWritable((float)prob));
			positiveNum += 1;
		} else {
			throw new IllegalStateException("error y in mr_test_mapper");
		}
	}
	
	@Override
	public void cleanup(Context context) throws IOException, InterruptedException {
		context.write(new IntWritable(0), new FloatWritable(Float.intBitsToFloat(negativeNum)));
		context.write(new IntWritable(1), new FloatWritable(Float.intBitsToFloat(positiveNum)));
	}
	
}
