package parallel.train;

import parallel.config.Config;
import parallel.linesearch.LineSearchBacktracking;
import parallel.linesearch.LineSearchFunction;
import parallel.linesearch.LineSearchOwlqn;
import parallel.lossfunction.LogisticRegression;
import parallel.lossfunction.LogisticRegressionInMemory;
import parallel.lossfunction.LogisticRegressionLessMemory;
import parallel.lossfunction.LossFunction;
import parallel.progress.Progress;
import parallel.progress.ProgressFunction;
import parallel.test.MRTest;
import parallel.test.TestFunction;
import parallel.test.TestInMemory;
import parallel.vecfree.MRVecFree;
import parallel.vecfree.VecFreeFunction;
import parallel.vecfree.VecFreeLocal;

public class Main {
	public static void main(String[] args) throws Exception {
		Config config = new Config();
		config.setConfigFile("config_file");
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
