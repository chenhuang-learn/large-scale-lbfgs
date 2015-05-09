package hc.parallel.config;

public enum LineSearchConstant {
	/**
	 * backtracking method for l1-regularization.
	 */
	LBFGS_LINESEARCH_BACKTRACKING_OWLQN,
	/**
	 * Backtracking method with the Armijo condition. The backtracking method
	 * finds the step length such that it satisfies the sufficient decrease
	 * (Armijo) condition, f(x + stp * d) <= f(x) + ftol * stp * g(x)^T d,
	 * 
	 * where x is the current point, d is the current search direction, and stp is
	 * the step length.
	 */
	LBFGS_LINESEARCH_BACKTRACKING_ARMIJO,
	/**
	 * Backtracking method with regular Wolfe condition. The backtracking method
	 * finds the step length such that it satisfies both the Armijo condition
	 * (LBFGS_LINESEARCH_BACKTRACKING_ARMIJO) and the curvature condition
	 * g(x + stp * d)^T d >= wolfe * g(x)^T d,
	 * 
	 * where x is the current point, d is the current search direction, and stp is
	 * the step length.
	 */
	LBFGS_LINESEARCH_BACKTRACKING_WOLFE,
	/**
	 * Backtracking method with strong Wolfe condition. The backtracking method
	 * finds the step length such that it satisfies both the Armijo condition
	 * (LBFGS_LINESEARCH_BACKTRACKING_ARMIJO) and the following condition
	 * |g(x + stp * d)^T d| <= wolfe * |g(x)^T d|,
	 * 
	 * where x is the current point, d is the current search direction, and a is
	 * the step length.
	 */
	LBFGS_LINESEARCH_BACKTRACKING_STRONG_WOLFE;
}
