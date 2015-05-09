package hc.parallel.train;

import hc.parallel.config.Config;
import hc.parallel.linesearch.LineSearchBacktracking;
import hc.parallel.linesearch.LineSearchFunction;
import hc.parallel.linesearch.LineSearchOwlqn;
import hc.parallel.lossfunction.LogisticRegression;
import hc.parallel.lossfunction.LogisticRegressionInMemory;
import hc.parallel.lossfunction.LogisticRegressionLessMemory;
import hc.parallel.lossfunction.LossFunction;
import hc.parallel.progress.Progress;
import hc.parallel.progress.ProgressFunction;
import hc.parallel.test.MRTest;
import hc.parallel.test.TestFunction;
import hc.parallel.test.TestInMemory;
import hc.parallel.vecfree.MRVecFree;
import hc.parallel.vecfree.VecFreeFunction;
import hc.parallel.vecfree.VecFreeLocal;

public class Main {
	public static void main(String[] args) throws Exception {
		if(args.length != 1) {
			System.out.println("run a local job:");
			System.out.println("java -jar <xxx.jar> <config_file>");
			System.out.println("run a hadoop job:");
			System.out.println("hadoop jar <xxx.jar> <config_file>");
		}
		
		Config config = new Config();
		config.setConfigFile(args[0]);
		System.out.println(config);
		
		LossFunction lossFunction = null;
		LineSearchFunction lineSearchFunction = null;
		VecFreeFunction vecFreeFunction = null;
		TestFunction testFunction = null;
		if(config.localJob) {
			lossFunction = new LogisticRegressionInMemory(config);
			vecFreeFunction = new VecFreeLocal(config); 
			testFunction = new TestInMemory(config);
		} else {
			if(config.lessMemory) {
				lossFunction = new LogisticRegressionLessMemory(config);
			} else {
				lossFunction = new LogisticRegression(config);
			}
			vecFreeFunction = new MRVecFree(config);
			testFunction = new MRTest(config);
		}
		
		if(config.param.orthantwiseC == 0.f) {
			lineSearchFunction = new LineSearchBacktracking(config, lossFunction);
		} else {
			lineSearchFunction = new LineSearchOwlqn(config, lossFunction);
		}
		
		ProgressFunction progress = new Progress(config);
		
		float[] x = new float[config.dataDimensions];
		float[] ptrFx = new float[1];
		LBFGS.lbfgs(config, x, ptrFx, lossFunction, lineSearchFunction, progress, vecFreeFunction, testFunction);
		
	}
}
