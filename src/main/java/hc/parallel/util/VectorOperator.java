package hc.parallel.util;

public class VectorOperator {
	
	public static float vecdot(float[] x, float[] y) {
		if(x.length != y.length) {
			throw new IllegalStateException("vector size error in vecdot");
		}
		double re = 0;
		for(int i=0; i<x.length; i++) {
			re += x[i] * y[i];
		}
		return (float)re;
	}
	
	public static float vec2norm(float[] x) {
		return (float) Math.sqrt(vecdot(x, x));
	}
	
	public static float vec2norminv(float[] x) {
		return 1.f / vec2norm(x);
	}
	
	public static void veccpy(float[] dest, float[] src) {
		if(dest.length != src.length) {
			throw new IllegalStateException("vec size error in veccpy");
		}
		System.arraycopy(src, 0, dest, 0, dest.length);
	}
	
	public static void vecncpy(float[] dest, float[] src) {
		if(dest.length != src.length) {
			throw new IllegalStateException("vec size error in vecncpy");
		}
		for (int i = 0; i < dest.length; i++) {
			dest[i] = -src[i];
		}
	}
	
	public static void vecadd(float[] dest, float[] src, float c) {
		if(dest.length != src.length) {
			throw new IllegalStateException("vec size error in vecadd");
		}
		for (int i = 0; i < dest.length; i++) {
			dest[i] += c * src[i];
		}
	}
	
	public static void vecdiff(float[] dest, float[] src1, float[] src2) {
		for (int i = 0; i < dest.length; i++) {
			dest[i] = src1[i] - src2[i];
		}
	}
	
	public static void owlqnProject(float[] x, float[] xp, float[] gp, int start, int end) {
		for(int i=start; i<end; i++) {
			float sign = (xp[i] == 0.f) ? -gp[i] : xp[i];
			if(x[i] * sign <= 0) {
				x[i] = 0;
			}
		}
	}
	
	public static float owlqnL1Norm(float[] x, int start, int end) {
		double norm = 0.f;
		for(int i=start; i<end; i++) {
			norm += Math.abs(x[i]);
		}
		return (float) norm;
	}
	
	public static void owlqnPseudoGradient(float[] pg, float[] x, float[] g, int n,
			float c, int start, int end) {
		int i;
		for(i=0; i<start; i++) {
			pg[i] = g[i];
		}
		for(i=start; i<end; i++) {
			if(x[i] < 0.) {
				pg[i] = g[i] - c;
			} else if(x[i] > 0.) {
				pg[i] = g[i] + c;
			} else {
				if(g[i] < -c) {
					pg[i] = g[i] + c;
				} else if(g[i] > c) {
					pg[i] = g[i] - c;
				} else {
					pg[i] = 0.f;
				}
			}
		}
		for(i=end; i<n; i++) {
			pg[i] = g[i];
		}
	}
	
}
