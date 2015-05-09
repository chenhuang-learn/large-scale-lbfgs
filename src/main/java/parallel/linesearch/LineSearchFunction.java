package parallel.linesearch;

public interface LineSearchFunction {
	
	int lineSearch(float[] x, float[] f, float[] g, float[] s,
			float[] stp, float[] xp, float[] gp, int k) throws Exception;
	
}
