package parallel.lossfunction;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.avro.file.DataFileReader;
import org.apache.avro.io.DatumReader;
import org.apache.avro.specific.SpecificDatumReader;

import parallel.avro.entry;
import parallel.avro.lbfgsdata;
import parallel.config.Config;

public class LogisticRegressionInMemory implements LossFunction {
	
	final private Config config;
	private List<lbfgsdata> data = new ArrayList<lbfgsdata>();;
	
	public LogisticRegressionInMemory(Config config) throws IOException {
		this.config = config;
		
		String fileName = config.dataPath;
		DatumReader<lbfgsdata> datumReader = new SpecificDatumReader<lbfgsdata>(lbfgsdata.class);
		DataFileReader<lbfgsdata> dataFileReader = new DataFileReader<lbfgsdata>(new File(fileName), datumReader);
		while(dataFileReader.hasNext()) {
			lbfgsdata d = dataFileReader.next();
			data.add(d);
		}
		dataFileReader.close();
	}
	
	@Override
	public float evaluate(float[] w, float[] g, int k) throws Exception {
		double loss = 0.0;
		double[] g_accuracy = new double[g.length];
		for (int i = 0; i < g_accuracy.length; i++) { g_accuracy[i] = 0.f; }
		float l2_c = config.l2_c;
		
		for (lbfgsdata d : data) {	
			float weight = d.getWeight();
			float offset = d.getOffset();
			int y = d.getResponse();
			List<entry> l = d.getFeatures();
			
			double decision_value = 0.0;
			for(entry e : l) {
				decision_value += e.getValue() * w[e.getIndex()];
			}
			decision_value += offset;
			
			double loss_temp;
			loss_temp = y * decision_value;
			if(loss_temp > 0) {
				loss_temp = Math.log(1 + Math.exp(-loss_temp));
			} else {
				loss_temp = ((-loss_temp) + Math.log(1 + Math.exp(loss_temp)));
			}
			loss += weight * loss_temp;
			
			double gradient_temp = ((1 / (1 + Math.exp(-y * decision_value)) - 1) * y * weight);
			for(entry e : l) {
				g_accuracy[e.getIndex()] += gradient_temp * e.getValue();
			}
		}
		for (int i = 0; i < w.length; i++) {
			loss += 0.5 * l2_c * w[i] * w[i];
			g_accuracy[i] += l2_c * w[i];
		}
		
		for (int i = 0; i < g.length; i++) {
			g[i] = (float) g_accuracy[i];
		}
		return (float) loss;
	}

}
