package hc.parallel.train;

import hc.parallel.config.Config;
import hc.parallel.linesearch.LineSearchFunction;
import hc.parallel.lossfunction.LossFunction;
import hc.parallel.progress.ProgressFunction;
import hc.parallel.test.TestFunction;
import hc.parallel.test.TestResult;
import hc.parallel.util.VectorOperator;
import hc.parallel.vecfree.VecFreeFunction;

public class LBFGS {
	
	public static void lbfgs(Config config, float[] x, float[] ptrFx,
			LossFunction lossFunction, LineSearchFunction lineSearchFunction,
			ProgressFunction progressFunction, VecFreeFunction vecFreeFunction,
			TestFunction testFunction) throws Exception {
		
		final int m = config.param.m;
		int n = config.dataDimensions;
		
		float[][] b = new float[2*m+1][2*m+1];
		float[] alpha = new float[2*m+1];
		float[] theta = new float[2*m+1];
		
		float[] g = new float[n];
		float[] d = new float[n];
		float[] xp = new float[n];
		float[] gp = new float[n];
		float[] pg = null;
		float[] stp = new float[1];
		float[] fx = new float[1];
		float[] pf = null;
		float xnorm = 0.f, gnorm = 0.f, rate = 0.f, beta;
		int ls, real_m;
		if(config.param.orthantwiseC > 0) {
			pg = new float[n];
		}
		if(config.param.past > 0) {
			pf = new float[config.param.past];
		}
		
		int k = 0;
		// use x to calculate new g and fx, assume no foreseeable exception
		// when use l1 norm, fx must add |x|, g must transform to pg(pseudo gradient)
		fx[0] = lossFunction.evaluate(x, g, k);
		if(config.param.orthantwiseC > 0) {
			xnorm = VectorOperator.owlqnL1Norm(x, config.param.orthantwiseStart, config.param.orthantwiseEnd);
			fx[0] += xnorm * config.param.orthantwiseC;
			VectorOperator.owlqnPseudoGradient(pg, x, g, n, config.param.orthantwiseC, config.param.orthantwiseStart, config.param.orthantwiseEnd);
		}
		
		if(pf != null) { pf[0] = fx[0]; }
		
		if(config.param.orthantwiseC == 0.f) {
			VectorOperator.vecncpy(d, g);
		} else {
			VectorOperator.vecncpy(d, pg);
		}
		
		xnorm = VectorOperator.vec2norm(x);
		if(xnorm < 1.f) { xnorm = 1.f; }
		
		if(config.param.orthantwiseC == 0.f) {
			gnorm = VectorOperator.vec2norm(g);
		} else {
			gnorm = VectorOperator.vec2norm(pg);
		}
		
		if(gnorm / xnorm <= config.param.epsilon) {
			System.out.println("Initial Convergence");
			if(ptrFx != null) {
				ptrFx[0] = fx[0];
			}
			return;
		}
		
		stp[0] = VectorOperator.vec2norminv(d);
		
		k += 1;
		while(true) {
			System.out.println("Iteration " + k + " start!");
			
			VectorOperator.veccpy(xp, x);
			VectorOperator.veccpy(gp, g);
			
			// line search, get new x, fx, g|pg
			try {
				if(config.param.orthantwiseC == 0.f) {
					ls = lineSearchFunction.lineSearch(x, fx, g, d, stp, xp, gp, k);
				} else {
					ls = lineSearchFunction.lineSearch(x, fx, g, d, stp, xp, pg, k);
					VectorOperator.owlqnPseudoGradient(pg, x, g, n,
							config.param.orthantwiseC, config.param.orthantwiseStart, config.param.orthantwiseEnd);
				}
			} catch(Exception e) {
				System.err.println(e.getMessage());
				VectorOperator.veccpy(x, xp);
				if(ptrFx != null) {
					ptrFx[0] = fx[0];
				}
				return;
			}
			
			// test convergence
			xnorm = VectorOperator.vec2norm(x);
			gnorm = config.param.orthantwiseC == 0.f ? VectorOperator.vec2norm(g) : VectorOperator.vec2norm(pg);
			
			TestResult result = testFunction.test(x, config.testThreshold, k);
			
			if(progressFunction != null) {
				int exitCode = 0;
				if(config.param.orthantwiseC == 0.f) {
					exitCode = progressFunction.progress(x, g, fx[0], xnorm, gnorm, stp[0], k, ls);
				} else {
					exitCode = progressFunction.progress(x, pg, fx[0], xnorm, gnorm, stp[0], k, ls);
				}
				if(exitCode != 0) {
					System.out.printf("exit in after progress, exitcode : %d\n", exitCode);
					if(ptrFx != null) {
						ptrFx[0] = fx[0];
					}
					return;
				}
				exitCode = progressFunction.testResultProgress(result, k);
				if(exitCode != 0) {
					System.out.printf("exit in after testResultProgress, exitcode : %d\n", exitCode);
					if(ptrFx != null) {
						ptrFx[0] = fx[0];
					}
					return;
				}
			}
			
			if(xnorm < 1.f) { xnorm = 1.f; }
			if(gnorm / xnorm < config.param.epsilon) {
				System.out.println("Convergence");
				if(ptrFx != null) {
					ptrFx[0] = fx[0];
				}
				return;
			}
			if(pf!= null) {
				if(k >= config.param.past) {
					rate = (pf[k % config.param.past] - fx[0]) / fx[0];
					if(rate < config.param.delta) {
						System.out.println("Stopping Criterion");
						if(ptrFx != null) {
							ptrFx[0] = fx[0];
						}
						return;
					}
				}
				pf[k % config.param.past] = fx[0];
			}
			if(config.param.maxIterations != 0 && config.param.maxIterations <= k) {
				System.out.println("Max Number of Iterations");
				if(ptrFx != null) {
					ptrFx[0]= fx[0];
				}
				return;
			}
			
			// get d
			vecFreeFunction.vecsDot(b, k, x, xp, g, gp,
					config.param.orthantwiseC == 0.f ? g : pg);
			/*****
			for(int i=0; i<b.length; i++) {
				System.out.println(Arrays.toString(b[i]));
			}
			System.out.println();
			****/
			
			real_m = (k >= m) ? m : k;
			for (int i = 0; i < theta.length; i++) {
				theta[i] = (i == 2 * real_m) ? -1.f : 0.f;
			}
			// s[k-m]-->0, s[k-1]-->real_m-1, y[k-m]->real_m, y[k-1]->2*real_m-1
			// for i=k-1 to k-m, do: alpha[i]=s[i]*p/s[i]*y[i], p=p-alpha[i]*y[i]
			for (int i = real_m - 1; i >= 0; i--) {
				alpha[i] = 0;
				for (int j = 0; j < 2 * real_m + 1; j++) {
					alpha[i] += theta[j] * b[j][i];
				}
				alpha[i] /= b[i][i+real_m];
				theta[i+real_m] -= alpha[i];
			}
			// p = p * (s[k-1]*y[k-1]) / (y[k-1]*y[k-1])
			for (int i = 0; i < 2 * real_m + 1; i++) {
				theta[i] *= (b[real_m-1][2*real_m-1] / b[2*real_m-1][2*real_m-1]);
			}
			// for i=k-m to k-1, do: beta=y[i]*p/s[i]y[i], p=p+(alpha[i]-beta)*s[i]
			for (int i = 0; i < real_m; i++) {
				beta = 0.f;
				for (int j = 0; j < 2 * real_m + 1; j++) {
					beta += theta[j] * b[i+real_m][j];
				}
				beta /= b[i][i+real_m];
				theta[i] += (alpha[i] - beta);
			}
			
			// System.out.println(Arrays.toString(theta));
			// System.out.println();
			
			vecFreeFunction.computeDirect(d, theta, k,
					config.param.orthantwiseC == 0.f ? g : pg);
			
			if(config.param.orthantwiseC != 0.f) {
				for (int i = config.param.orthantwiseStart; i < config.param.orthantwiseEnd; i++) {
					if(d[i] * pg[i] >= 0) {
						d[i] = 0.f;
					}
				}
			}
			
			stp[0] = 1.f;
			System.out.println("Iteration " + k + " end!");
			k++;
		}
		
	}
	
}
