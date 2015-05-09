package hc.parallel.vecfree;

public interface VecFreeFunction {

	void vecsDot(float[][] b, int k, float[] x, float[] xp, float[] g,
			float[] gp, float[] pg) throws Exception;

	void computeDirect(float[] d, float[] theta, int k, float[] pg) throws Exception;

}

