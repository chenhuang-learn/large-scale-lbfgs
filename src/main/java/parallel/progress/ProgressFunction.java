package parallel.progress;

import parallel.test.TestResult;


public interface ProgressFunction {

	/**
	 * Callback interface to receive the progress of the optimization process.
	 * 
	 * The lbfgs() function call this function for each iteration. Implementing
	 * this function, a client program can store or display the current progress
	 * of the optimization process.
	 * 
	 * @param x
	 *            The current values of variables.
	 * @param g
	 *            The current gradient values of variables.
	 * @param fx
	 *            The current value of the objective function.
	 * @param xnorm
	 *            The Euclidean norm of the variables.
	 * @param gnorm
	 *            The Euclidean norm of the gradients.
	 * @param step
	 *            The line-search step used for this iteration.
	 * @param k
	 *            The iteration count.
	 * @param ls
	 *            The number of evaluations called for this iteration.
	 * @return int Zero to continue the optimization process. Returning a
	 *         non-zero value will cancel the optimization process.
	 */
	int progress(float[] x, float[] g, float fx, float xnorm, float gnorm,
			float step, int k, int ls);
	
	int testResultProgress(TestResult testResult, int k);
}
