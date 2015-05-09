package hc.parallel.progress;

import hc.parallel.config.Config;
import hc.parallel.test.TestResult;

import java.io.FileNotFoundException;
import java.io.PrintStream;
import java.text.SimpleDateFormat;
import java.util.Date;

public class Progress implements ProgressFunction {
	
    public static final int FEATURE_PRINT_NUM = 10;
    private PrintStream log;

    public Progress(Config config) {
    	if (log == null) {
            String logPath = config.logFile;
            try {
            	log = logPath.toLowerCase().equals("stdout") ? System.out : new PrintStream(logPath);
            } catch (FileNotFoundException e) {
                e.printStackTrace();
            }
        }
    }

    @Override
    public int progress(float[] x, float[] g, float fx, float xnorm, float gnorm, float step, int k, int ls) {
        log.println(stringFormat(x, g, fx, xnorm, gnorm, step, k, ls));
        return 0;
    }

    private String stringFormat(float[] x, float[] g, float fx, float xnorm, float gnorm, float step, int k, int ls) {
        SimpleDateFormat dateformat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

        String format = String.format("Iteration %d at time %s:\n\tfx=%f, xnorm=%f, gnorm=%f, ls=%d, step=%f\n",
                k, dateformat.format(new Date()), fx, xnorm, gnorm, ls, step);

        format += stringFormatVec(x, "x") +  stringFormatVec(g, "g");

        return format;

    }

    private String stringFormatVec(float[] vec,String desc) {
        String format = "";
        format += String.format("\t%s size=%d\n\t\t", desc, vec.length);

        int step = vec.length / FEATURE_PRINT_NUM;
        step = step == 0 ? 1 : step;

        for (int i = 0; i < vec.length; i += step) {
            format += String.format("%s[%d]=%f  ", desc, i, vec[i]);
        }
        format += System.lineSeparator();
        return format;
    }

	@Override
	public int testResultProgress(TestResult testResult, int k) {
		log.println("Iteration " + k + ":");
		log.println(testResult.toPrintString());
		log.println();
		return 0;
	}
    
}
