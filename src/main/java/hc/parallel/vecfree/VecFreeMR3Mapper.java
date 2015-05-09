package hc.parallel.vecfree;

import hc.parallel.avro.entry;

import java.io.IOException;

import org.apache.avro.mapred.AvroKey;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class VecFreeMR3Mapper extends Mapper<AvroKey<entry>, NullWritable, IntWritable, FloatWritable> {
	
	private float scale = 0.f;
	
	@Override
	public void setup(Context context) {
		Configuration conf = context.getConfiguration();
		String s_y_file_names = conf.get("s_y_file_names");
		int real_m = conf.getInt("real_m", 0);
		String[] s_y_files = MRVecFree.stringToStringArray(s_y_file_names);
		if((real_m * 2 + 1) != s_y_files.length) {
			throw new IllegalStateException("s_y_files.length is not equal with 2*real_m+1");
		}
		FileSplit split = (FileSplit)context.getInputSplit();
		int splitIndex = -1;
		for (int i = 0; i < s_y_files.length; i++) {
			if(split.getPath().getName().equals(s_y_files[i])) {
				splitIndex = i;
			}
		}
		scale = conf.getFloat("theta_" + splitIndex, 0.f);
	}
	
	@Override
	public void map(AvroKey<entry> key, NullWritable value, Context context) throws IOException, InterruptedException {
		entry e = key.datum();
		float v = e.getValue() * scale;
		if(v != 0.f) {
			context.write(new IntWritable(e.getIndex()), new FloatWritable(v));
		}
	}
	
}
