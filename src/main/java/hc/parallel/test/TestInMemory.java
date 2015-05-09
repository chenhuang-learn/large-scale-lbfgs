package hc.parallel.test;

import hc.parallel.avro.entry;
import hc.parallel.avro.lbfgsdata;
import hc.parallel.config.Config;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.avro.file.DataFileReader;
import org.apache.avro.io.DatumReader;
import org.apache.avro.specific.SpecificDatumReader;

public class TestInMemory implements TestFunction {
	
	private List<lbfgsdata> datas = new ArrayList<lbfgsdata>();
	private int numPositive = 0;
	private int numNegative = 0;
	
	public TestInMemory(Config config) throws IOException {
		String testDataPath = config.testDataPath;
		DatumReader<lbfgsdata> datumReader = new SpecificDatumReader<lbfgsdata>(lbfgsdata.class);
		DataFileReader<lbfgsdata> dataFileReader = new DataFileReader<lbfgsdata>(new File(testDataPath), datumReader);
		lbfgsdata data = null;
		while (dataFileReader.hasNext()) {
			data = dataFileReader.next();
			if(data.getResponse() == -1) {
				numNegative += 1;
			} else {
				numPositive += 1;
			}
			datas.add(data);
		}
		dataFileReader.close();
	}
	
	@Override
	public TestResult test(float[] x, float threshold, int k) throws Exception {
		float[] positiveProbs = new float[numPositive];
		float[] negativeProbs = new float[numNegative];
		
		int positiveIndex = 0, negativeIndex = 0;
		for (lbfgsdata data : datas) {
			int y = data.getResponse();
			List<entry> l = data.getFeatures();
			float offset = data.getOffset();
			
			double prob = 0.0;
			for(entry e : l) {
				prob += e.getValue() * x[e.getIndex()];
			}
			prob += offset;
			prob = (1 / (1 + Math.exp(-prob)));
			
			if(y == -1) {
				negativeProbs[negativeIndex] = (float)prob;
				negativeIndex += 1;
			} else {
				positiveProbs[positiveIndex] = (float)prob;
				positiveIndex += 1;
			}
		}
		return new TestResult().calTestResult(positiveProbs, negativeProbs, threshold);
	}
	
}
