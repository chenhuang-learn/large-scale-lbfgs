package hc.parallel.lossfunction;

public interface LossFunction {

	/**
	 * Callback interface to provide objective function and gradient
	 * evaluations.
	 * 
	 * call this function to obtain the values of objective
	 * function and its gradients when needed. A client program must implement
	 * this function to evaluate the values of the objective function and its
	 * gradients, given current values of variables.
	 * 
	 * @param w
	 *            The current values of variables.
	 * @param g
	 *            The gradient vector. The callback function must compute the
	 *            gradient values for the current variables.
	 * @param k
	 *            The iteration number.
	 *            
	 * @return lbfgsfloatval_t The value of the objective function for the
	 *         current variables.
	 */
	float evaluate(float[] w, float[] g, int k) throws Exception;
}
