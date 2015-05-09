package hc.parallel.vecfree;

import hc.parallel.config.Config;
import hc.parallel.util.VectorOperator;

public class VecFreeLocal implements VecFreeFunction {
	
	private final Config config;
	private final float[][] s;
	private final float[][] y;
	
	public VecFreeLocal(Config config) {
		this.config = config;
		this.s = new float[config.param.m][config.dataDimensions];
		this.y = new float[config.param.m][config.dataDimensions];
	}
	
	@Override
	public void vecsDot(float[][] b, int k, float[] x, float[] xp, float[] g,
			float[] gp, float[] pg) {
		// save s(k-1) and y(k-1)
		int m = config.param.m;
		int end = (k-1) % m;
		VectorOperator.vecdiff(s[end], x, xp);
		VectorOperator.vecdiff(y[end], g, gp);
		
		// when bound=1, b_new(effective_size)=3*3
		// when bound=m, b_new(effective_size)=(2m+1)*(2m+1)
		int real_m = k >= m ? m : k;
		if(k <= m) {
			// b from (2*(real_m-1)+1)^2 to (2*real_m+1)^2
			// map (2*(real_m-1))^2 matrix to b_new
			for(int i=2*(real_m-1)-1; i>=0; i--) {
				for(int j=2*(real_m-1)-1; j>=0; j--) {
					int new_i = (i < real_m-1) ? i : (i+1);
					int new_j = (j < real_m-1) ? j : (j+1);
					b[new_i][new_j] = b[i][j];
				}
			}
		} else {
			for(int i=1; i<2*real_m; i++) {
				for(int j=1; j<2*real_m; j++) {
					if(i==real_m || j==real_m) { continue; }
					int new_i = i-1;
					int new_j = j-1;
					b[new_i][new_j] = b[i][j];
				}
			}
		}
		
		// calculate new b for new s, y, pg
		// i -> s|y -> index_s_y, s[end],y[end],pg->index
		int index_s_end = real_m - 1;
		int index_y_end = 2 * real_m -1;
		int index_pg = 2 * real_m;
		for(int i=0; i<2*(real_m-1); i++) {
			int index_b = (i < real_m-1) ? i : (i+1);
			float[][] tmp = (i < real_m-1) ? s : y;
			int index_s_y = (i < real_m-1) ? i : (i-(real_m-1));
			index_s_y = end - (real_m - 1) + index_s_y;
			if(index_s_y < 0) {
				index_s_y = m + index_s_y;
			}
			
			b[index_s_end][index_b] = VectorOperator.vecdot(s[end], tmp[index_s_y]);
			b[index_b][index_s_end] = b[index_s_end][index_b];
			
			b[index_y_end][index_b] = VectorOperator.vecdot(y[end], tmp[index_s_y]);
			b[index_b][index_y_end] = b[index_y_end][index_b];
			
			b[index_pg][index_b] = VectorOperator.vecdot(pg, tmp[index_s_y]);
			b[index_b][index_pg] = b[index_pg][index_b];
		}
		
		b[index_s_end][index_s_end] = VectorOperator.vecdot(s[end], s[end]);
		b[index_y_end][index_y_end] = VectorOperator.vecdot(y[end], y[end]);
		b[index_pg][index_pg] = VectorOperator.vecdot(pg, pg);
		
		b[index_pg][index_s_end] = VectorOperator.vecdot(pg, s[end]);
		b[index_s_end][index_pg] = b[index_pg][index_s_end];
		b[index_pg][index_y_end] = VectorOperator.vecdot(pg, y[end]);
		b[index_y_end][index_pg] = b[index_pg][index_y_end];
		b[index_s_end][index_y_end] = VectorOperator.vecdot(s[end], y[end]);
		b[index_y_end][index_s_end] = b[index_s_end][index_y_end];
	}

	@Override
	public void computeDirect(float[] d, float[] theta, int k, float[] pg) {
		int m = config.param.m;
		int end = (k-1) % m;
		int real_m = m <= k ? m : k;
		
		for (int i = 0; i < d.length; i++) {
			d[i] = 0.f;
		}
		
		// i -> s|y -> index_tmp
		for (int i = 0; i < 2 * real_m; i++) {
			float[][] tmp = i < real_m ? s : y;
			int index_tmp = i < real_m ? i : (i-real_m);
			index_tmp = end - (real_m - 1) + index_tmp;
			if(index_tmp < 0) {
				index_tmp = index_tmp + m;
			}
			VectorOperator.vecadd(d, tmp[index_tmp], theta[i]);
		}
		VectorOperator.vecadd(d, pg, theta[2*real_m]);
	}

}
