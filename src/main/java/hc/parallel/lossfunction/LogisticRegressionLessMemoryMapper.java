package hc.parallel.lossfunction;

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

public class LogisticRegressionLessMemoryMapper extends Mapper<AvroKey<lbfgsdata>, NullWritable, IntWritable, FloatWritable> {
	
	private float[] weight = null;
	private double loss =  0.0;
	
	@Override
	public void setup(Context context) throws IOException {
		// get lbfgs_data_max_index
		int max_index = context.getConfiguration().getInt("lbfgs_data_max_index", -2);
		weight = new float[max_index + 1];
		// read weightFile in distributed cache
		FileOperator.readArrayLocal(new Path("weightFile"), context.getConfiguration(), weight);
		System.out.println("less memory lr");
	}
	
	// loss = add( w[i] * log(1+exp(-y[i]* weightT * x[i])) ) + 0.5 * lbfgs_l2_c * ||weight||2
	@Override
	public void map(AvroKey<lbfgsdata> key, NullWritable nullvalue, Context context) throws IOException, InterruptedException {
		lbfgsdata data = key.datum();
		
		float w = data.getWeight();
		float offset = data.getOffset();
		int y = data.getResponse();
		List<entry> l = data.getFeatures();
		
		double decision_value = 0.0;
		for(entry e : l) {
			decision_value += e.getValue() * weight[e.getIndex()];
		}
		decision_value += offset;
		
		double loss_temp;
		loss_temp = y * decision_value;
		if(loss_temp > 0){
			loss_temp = Math.log(1 + Math.exp(-loss_temp));
		} else {
			loss_temp = ((-loss_temp) + Math.log(1 + Math.exp(loss_temp)));
		}
		loss += w * loss_temp;
		
		double gradient_temp = (1 / (1 + Math.exp(-y * decision_value)) - 1) * y * w;
		for(entry e : l) {
			context.write(new IntWritable(e.getIndex()), new FloatWritable((float)(gradient_temp * e.getValue())));
		}
	}
	
	@Override
	public void cleanup(Context context) throws IOException, InterruptedException {
		context.write(new IntWritable(-1), new FloatWritable((float)loss));
	}
	
}
