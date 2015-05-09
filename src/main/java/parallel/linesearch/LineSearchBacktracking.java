package parallel.linesearch;

import parallel.config.Config;
import parallel.config.LineSearchConstant;
import parallel.lossfunction.LossFunction;
import parallel.util.VectorOperator;

public class LineSearchBacktracking implements LineSearchFunction {
	
	private final Config config;
	private final LossFunction lossFunction;
	
	public LineSearchBacktracking(Config config, LossFunction lossFunction) {
		this.config = config;
		this.lossFunction = lossFunction;
	}
	
	@Override
	public int lineSearch(float[] x, float[] f, float[]g, float[] s,
			float[] stp, float[] xp, float[] gp, int k) throws Exception {
		
		int count = 0;
		float width, finit;
		double dg, dginit, dgtest;
		final float dec = 0.5f, inc = 2.1f;
		
		if(stp[0] <= 0) {
			throw new IllegalStateException("stp must > 0 in linesearch");
		}
		dginit = VectorOperator.vecdot(g, s);
		if(dginit >= 0) {
			throw new IllegalStateException("g*s must < 0 in linesearch");
		}
		
		finit = f[0];
		dgtest = config.param.ftol * dginit;
		 
		while(true) {
			VectorOperator.veccpy(x, xp);
			VectorOperator.vecadd(x, s, stp[0]);
			
			f[0] = lossFunction.evaluate(x, g, k);
			
			count += 1;
			if(f[0] > finit + stp[0] * dgtest) {
				width = dec;
			} else {
				if(config.param.linesearch == LineSearchConstant.LBFGS_LINESEARCH_BACKTRACKING_ARMIJO) {
					return count;
				}
				dg = VectorOperator.vecdot(g, s);
				if(dg < config.param.wolfe * dginit) {
					width = inc;
				} else {
					if(config.param.linesearch == LineSearchConstant.LBFGS_LINESEARCH_BACKTRACKING_WOLFE) {
						return count;
					}
					if(dg > -config.param.wolfe * dginit) {
						width = dec;
					} else {
						return count;
					}
				}
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
