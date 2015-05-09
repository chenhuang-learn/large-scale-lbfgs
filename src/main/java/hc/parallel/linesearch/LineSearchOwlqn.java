package hc.parallel.linesearch;

import hc.parallel.config.Config;
import hc.parallel.lossfunction.LossFunction;
import hc.parallel.util.VectorOperator;

public class LineSearchOwlqn implements LineSearchFunction {

	private final Config config;
	private final LossFunction lossFunction;
	
	public LineSearchOwlqn(Config config, LossFunction lossFunction) {
		this.config = config;
		this.lossFunction = lossFunction;
	}
	
	@Override
	public int lineSearch(float[] x, float[] f, float[] g, float[] s,
			float[] stp, float[] xp, float[] gp, int k) throws Exception {
		int count = 0;
		float width = 0.5f, norm = 0.f;
		float finit = f[0];
		double dgtest;
		
		if(stp[0] <= 0) {
			throw new IllegalStateException("stp must > 0 in linesearch");
		}
		
		while(true) {
			VectorOperator.veccpy(x, xp);
			VectorOperator.vecadd(x, s, stp[0]);
			VectorOperator.owlqnProject(x, xp, gp, config.param.orthantwiseStart, config.param.orthantwiseEnd);
			
			f[0] = lossFunction.evaluate(x, g, k);
			norm = VectorOperator.owlqnL1Norm(x, config.param.orthantwiseStart, config.param.orthantwiseEnd);
			f[0] += norm * config.param.orthantwiseC;
			
			count += 1;
			
			dgtest = 0.0;
			for(int i=0; i<x.length; i++) {
				dgtest += (x[i] - xp[i]) * gp[i];
			}
			if(f[0] <= finit + config.param.ftol * dgtest) {
				return count;
			}
			
			if(count >= config.param.maxLinesearch) {
				throw new IllegalStateException("LBFGSERR_MAXLINESEARCH");
			}
			if(stp[0] < config.param.minStep) {
				throw new IllegalStateException("LBFGSERR_MINSTEP");
			}
			if(stp[0] > config.param.maxStep) {
				throw new IllegalStateException("LBFGSERR_MAXSTEP");
			}
			
			stp[0] *= width;
		}
	}

}
