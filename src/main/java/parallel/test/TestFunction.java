package parallel.test;

public interface TestFunction {
	
	TestResult test(float[] x, float threshold, int k) throws Exception;
	
}
