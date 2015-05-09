package hc.parallel.test;

import java.io.IOException;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

public class MRTestReducer extends Reducer<IntWritable, FloatWritable, TestResult, NullWritable>{
	
	private float threshold = -1.f;
	private int negativeNum = -1;
	private int positiveNum = -1;
	private TestResult testResult = null;
	private float[] positiveProbs = null;
	private float[] negativeProbs = null;
	
	@Override
	public void setup(Context context) {
		threshold = context.getConfiguration().getFloat("lbfgs_test_threshold", -1.f);
		positiveNum = 0;
		negativeNum = 0;
		testResult = new TestResult();
	}
	
	@Override
	public void reduce(IntWritable key, Iterable<FloatWritable> values, Context context) {
		int num = key.get();
		if (num == 0) {
			for (FloatWritable f : values) {
				negativeNum += Float.floatToIntBits(f.get());
			}
		} else if (num == 1) {
			for (FloatWritable f : values) {
				positiveNum += Float.floatToIntBits(f.get());
			}
		} else if (num == 2) {
			negativeProbs = new float[negativeNum];
			int index = 0;
			for (FloatWritable f : values) {
				negativeProbs[index] = f.get();
				index++;
			}
		} else if (num == 3) {
			positiveProbs = new float[positiveNum];
			int index = 0;
			for (FloatWritable f : values) {
				positiveProbs[index] = f.get();
				index++;
			}
		} else {
			throw new IllegalStateException("error key in mr_test_reducer : " + num);
		}
	}
	
	@Override
	public void cleanup(Context context) throws IOException, InterruptedException {
		testResult.calTestResult(positiveProbs, negativeProbs, threshold);
		context.write(testResult, NullWritable.get());
	}
}
