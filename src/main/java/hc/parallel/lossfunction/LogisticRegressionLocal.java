package hc.parallel.lossfunction;

import hc.parallel.avro.entry;
import hc.parallel.avro.lbfgsdata;
import hc.parallel.config.Config;

import java.io.File;
import java.util.Arrays;
import java.util.List;
import java.util.Random;

import org.apache.avro.file.DataFileReader;
import org.apache.avro.io.DatumReader;
import org.apache.avro.specific.SpecificDatumReader;

public class LogisticRegressionLocal implements LossFunction {
	
	final private Config config;
	
	public LogisticRegressionLocal(Config config) {
		this.config = config;
	}
	
	@Override
	public float evaluate(float[] w, float[] g, int k) throws Exception {
		double loss = 0.0;
		double[] g_accuracy = new double[g.length];
		for (int i = 0; i < g_accuracy.length; i++) { g_accuracy[i] = 0.f; }
		
		float l2_c = config.l2_c;
		String fileName = config.dataPath;
		DatumReader<lbfgsdata> datumReader = new SpecificDatumReader<lbfgsdata>(lbfgsdata.class);
		DataFileReader<lbfgsdata> dataFileReader = new DataFileReader<lbfgsdata>(new File(fileName), datumReader);
		
		lbfgsdata data = null;
		while(dataFileReader.hasNext()) {
			data = dataFileReader.next(data);
			
			float weight = data.getWeight();
			float offset = data.getOffset();
			int y = data.getResponse();
			List<entry> l = data.getFeatures();
			
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
		
		dataFileReader.close();
		
		for (int i = 0; i < g.length; i++) {
			g[i] = (float) g_accuracy[i];
		}
		return (float)loss;
	}
	
	public static void main(String[] args) throws Exception {
		Config config = new Config();
		config.setConfigFile("config_file");
		LogisticRegressionLocal lr = new LogisticRegressionLocal(config);
		
		Random random = new Random(1);
		float[] w = new float[config.dataDimensions];
		for (int i = 0; i < w.length; i++) { w[i] = random.nextFloat(); }
		float[] g = new float[config.dataDimensions];
		
		System.out.println(Arrays.toString(w));
		System.out.println(lr.evaluate(w, g, 1));
		System.out.println(Arrays.toString(g));
	}

}
