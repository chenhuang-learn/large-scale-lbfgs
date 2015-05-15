package hc.parallel.config;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;

/**
 * @author chenhuang
 */
public class Config {
	
	private Map<String, String> m = null;
	
	public LBFGSParameter param = null;
	
	public String logFile = null;
	public boolean localJob = true;
	public float l2_c = -1.f;
	public String dataPath = null;
	public String testDataPath = null;
	public float testThreshold = -1.f;
	public int dataDimensions = -1;
	public int dataMaxIndex = -1;
	
	public String jobName = "large-scale-lbfgs";
	public String workingDirectory = null;
	public int mr1NumReduceTasks = 10;
	public boolean lessMemory = false;
	
	public Config() { }
	
	public void setConfigFile(String file) throws IOException {
		m = new HashMap<String, String>();
		BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream(file), "UTF-8"));
		Properties properties = new Properties();
		properties.load(br);
		br.close();
		for (String name : properties.stringPropertyNames()) {
			m.put(name, properties.getProperty(name));
		}
	
		setJobParameter();
		
		param = new LBFGSParameter();
		setLBFGSParameter();
	}
	
	private void setJobParameter() {
		if(m.containsKey("lbfgs_log_file")) {
			logFile = m.get("lbfgs_log_file");
		} else {
			logFile = "stdout";
		}
		if(m.containsKey("lbfgs_l2_c")) {
			l2_c = Float.parseFloat(m.get("lbfgs_l2_c"));
		} else {
			l2_c = 1.f;
		}
		if(l2_c < 0) {
			throw new IllegalStateException("lbfgs_l2_c must >= 0");
		}
		dataPath = m.get("lbfgs_data_path");
		testDataPath = m.get("lbfgs_test_data_path");
		dataMaxIndex = Integer.parseInt(m.get("lbfgs_data_max_index"));
		dataDimensions = dataMaxIndex + 1;
		if(dataDimensions <= 0) {
			throw new IllegalStateException("lbfgs_data_max_index must >= 0");
		}
		if(m.containsKey("lbfgs_test_threshold")) {
			testThreshold = Float.parseFloat(m.get("lbfgs_test_threshold"));
		} else {
			testThreshold = 0.5f;
		}
		
		String local = "true";
		if(m.containsKey("lbfgs_local")) {
			local = m.get("lbfgs_local").toLowerCase();
		}
		if(local.equals("true")) {
			localJob = true;
		} else if(local.equals("false")) {
			localJob = false;
		} else {
			throw new IllegalStateException("lbfgs_local must be true or false");
		}
		
		if(!localJob) {
			String less_memory = "false";
			if(dataDimensions > 1e8) {
				less_memory = "true";
			}
			if(m.containsKey("lbfgs_mr1_less_memory")) {
				less_memory = m.get("lbfgs_mr1_less_memory").toLowerCase();
			}
			if(less_memory.equals("true")) {
				lessMemory = true;
			} else if(less_memory.equals("false")) {
				lessMemory = false;
			} else {
				throw new IllegalStateException("lbfgs_mr1_less_memory must be true or false");
			}
			if(m.containsKey("lbfgs_job_name")) {
				jobName = m.get("lbfgs_job_name");
			}
			workingDirectory = m.get("lbfgs_working_directory");
			if(m.containsKey("lbfgs_mr1_num_reduce_tasks")) {
				mr1NumReduceTasks = Integer.parseInt(m.get("lbfgs_mr1_num_reduce_tasks"));
			}
		}
	}
	
	private void setLBFGSParameter() {
		param.m = m.containsKey("lbfgs_m") ? Integer.parseInt(m.get("lbfgs_m")) : param.m;
		param.epsilon = m.containsKey("lbfgs_epsilon") ? Float.parseFloat(m.get("lbfgs_epsilon")) : param.epsilon;
		if(param.epsilon <= 0) {
			throw new IllegalStateException("lbfgs_epsilon > 0");
		}
		param.past = m.containsKey("lbfgs_past") ? Integer.parseInt(m.get("lbfgs_past")) : param.past;
		if(param.past < 0) {
			throw new IllegalStateException("lbfgs_past must >= 0");
		}
		param.delta = m.containsKey("lbfgs_delta") ? Float.parseFloat(m.get("lbfgs_delta")) : param.delta;
		if(param.delta < 0) {
			throw new IllegalStateException("lbfgs_delta must >= 0");
		}
		param.maxIterations = m.containsKey("lbfgs_max_iterations") ? Integer.parseInt(m.get("lbfgs_max_iterations")) : param.maxIterations;
		if(param.maxIterations < 0) {
			throw new IllegalStateException("lbfgs_max_iterations must >= 0");
		}
		if(m.containsKey("lbfgs_line_search")) {
			String lineSearchName = m.get("lbfgs_line_search");
			if(lineSearchName.equals("backtracking_owlqn")) {
				param.linesearch = LineSearchConstant.LBFGS_LINESEARCH_BACKTRACKING_OWLQN;
			} else if(lineSearchName.equals("backtracking_armijo")) {
				param.linesearch = LineSearchConstant.LBFGS_LINESEARCH_BACKTRACKING_ARMIJO;
			} else if(lineSearchName.equals("backtracking_wolfe")) {
				param.linesearch = LineSearchConstant.LBFGS_LINESEARCH_BACKTRACKING_WOLFE;
			} else if(lineSearchName.equals("backtracking_strong_wolfe")) {
				param.linesearch = LineSearchConstant.LBFGS_LINESEARCH_BACKTRACKING_STRONG_WOLFE;
			} else {
				throw new IllegalStateException("Unknown linesearch method");
			}
		}
		param.maxLinesearch = m.containsKey("lbfgs_max_line_search") ? Integer.parseInt(m.get("lbfgs_max_line_search")) : param.maxLinesearch;
		if(param.maxLinesearch < 0) {
			throw new IllegalStateException("lbfgs_max_line_search must >= 0");
		}
		param.minStep = m.containsKey("lbfgs_min_step") ? Float.parseFloat(m.get("lbfgs_min_step")) : param.minStep;
		if(param.minStep <= 0) {
			throw new IllegalStateException("lbfgs_min_step must > 0");
		}
		param.maxStep = m.containsKey("lbfgs_max_step") ? Float.parseFloat(m.get("lbfgs_max_float")) : param.maxStep;
		if(param.maxStep <= 0) {
			throw new IllegalStateException("lbfgs_max_step must > 0");
		}
		param.ftol = m.containsKey("lbfgs_ftol") ? Float.parseFloat(m.get("lbfgs_ftol")) : param.ftol;
		if(param.ftol <= 0) {
			throw new IllegalStateException("lbfgs_ftol must > 0");
		}
		param.gtol = m.containsKey("lbfgs_gtol") ? Float.parseFloat(m.get("lbfgs_gtol")) : param.gtol;
		param.xtol = m.containsKey("lbfgs_xtol") ? Float.parseFloat(m.get("lbfgs_xtol")) : param.xtol;
		param.orthantwiseC = m.containsKey("lbfgs_l1_c") ? Float.parseFloat(m.get("lbfgs_l1_c")) : param.orthantwiseC;
		if(param.orthantwiseC < 0) {
			throw new IllegalStateException("lbfgs_l1_c must >= 0");
		} else if(param.orthantwiseC > 0) {
			if (param.linesearch == null) {
				param.linesearch = LineSearchConstant.LBFGS_LINESEARCH_BACKTRACKING_OWLQN;
			}
			if(param.linesearch != LineSearchConstant.LBFGS_LINESEARCH_BACKTRACKING_OWLQN) {
				throw new IllegalStateException("lbfgs_line_search must be backtracking_owlqn");
			}
		} else {
			if(param.linesearch == null) {
				param.linesearch = LineSearchConstant.LBFGS_LINESEARCH_BACKTRACKING_WOLFE;
			}
			if(param.linesearch == LineSearchConstant.LBFGS_LINESEARCH_BACKTRACKING_OWLQN) {
				throw new IllegalStateException("lbfgs_line_search can't be backtracking_owlqn");
			}
		}
		if(param.orthantwiseEnd < 0) {
			param.orthantwiseEnd = dataDimensions;
		}
		if(!(param.orthantwiseStart >= 0 && param.orthantwiseStart < param.orthantwiseEnd && param.orthantwiseEnd <= dataDimensions)) {
			throw new IllegalStateException("orthantwiseStart or orthantwiseEnd wrong");
		}
		param.wolfe = m.containsKey("lbfgs_wolfe") ? Float.parseFloat(m.get("lbfgs_wolfe")) : param.wolfe;
		if(param.linesearch == LineSearchConstant.LBFGS_LINESEARCH_BACKTRACKING_STRONG_WOLFE ||
				param.linesearch == LineSearchConstant.LBFGS_LINESEARCH_BACKTRACKING_WOLFE) {
			if(param.wolfe <= param.ftol || param.wolfe >= 1) {
				throw new IllegalStateException("lbfgs_wolfe must > lbfgs_ftol and < 1");
			}
		}
	}
	
	public void setMROptionalConfig(Configuration conf, String prefix) {
		for(String key : m.keySet()) {
			if(key.startsWith(prefix)) {
				int len = prefix.length();
				conf.set(key.substring(len), m.get(key));
			}
		}
	}
	
	public static void main(String[] args) throws IOException {
		Config config = new Config();
		config.setConfigFile("config_file");
		System.out.println(config);
	}

	@Override
	public String toString() {
		return "param=" + param + "\n"
				+ "logFile=" + logFile + ", localJob=" + localJob + ", l2_c=" + l2_c 
				+ ", dataPath=" + dataPath + ", testDataPath=" + testDataPath
				+ ", testThreshold=" + testThreshold + ", dataMaxIndex=" + dataMaxIndex	+ "\n"
				+ "jobName=" + jobName + ", workingDirectory="
				+ workingDirectory + ", mr1NumReduceTasks=" + mr1NumReduceTasks
				+ ", lessMemory=" + lessMemory;
	}
	
	
	
}
