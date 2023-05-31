package org.selab.jcodelib.coverage;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;

import org.jacoco.core.analysis.Analyzer;
import org.jacoco.core.analysis.CoverageBuilder;
import org.jacoco.core.analysis.IClassCoverage;
import org.jacoco.core.analysis.ICounter;
import org.jacoco.core.data.ExecutionDataReader;
import org.jacoco.core.data.ExecutionDataStore;
import org.jacoco.core.data.SessionInfoStore;

public class CoverageUtils {

	public List<LineCoverage> getCoverage(String coverageFile, File classDir, List<String> classNames)  throws FileNotFoundException, IOException {
		List<LineCoverage> coverage = new ArrayList<>();
		ExecutionDataStore executionData = new ExecutionDataStore();
		executionData = readExecutionData(coverageFile);

		String coverageInfo = "";

		CoverageBuilder coverageBuilder = new CoverageBuilder();
		Analyzer analyzer = new Analyzer(executionData, coverageBuilder);
		for (String className : classNames) {
			InputStream targetClass = getClass(classDir, className);
			analyzer.analyzeClass(targetClass, className);
		}

		for (IClassCoverage cc : coverageBuilder.getClasses()){
			for (int i = cc.getFirstLine(); i <= cc.getLastLine(); i++){
				int status = cc.getLine(i).getStatus();
				if(isCovered(status)){
					coverageInfo += ","+i;
				}
			}
		}
		if(coverageInfo.length() >= 1){
			coverageInfo = coverageInfo.substring(1);
		}

		return coverage;
	}

	public ExecutionDataStore readExecutionData(String fileName) throws FileNotFoundException, IOException {
		final FileInputStream in = new FileInputStream(new File(fileName));
		ExecutionDataStore executionDataStore = new ExecutionDataStore();
		SessionInfoStore sessionInfoStore = new SessionInfoStore();
		final ExecutionDataReader reader = new ExecutionDataReader(in);
		reader.setSessionInfoVisitor(sessionInfoStore);
		reader.setExecutionDataVisitor(executionDataStore);
		while(reader.read()){
		}
		in.close();

		return executionDataStore;
	}

	public boolean isCovered(final int status) {
		switch (status) {
		case ICounter.NOT_COVERED:
			return false;
		case ICounter.PARTLY_COVERED:
			return true;
		case ICounter.FULLY_COVERED:
			return true;
		}
		return false;
	}

	private InputStream getClass(File classDir, String className) throws IOException {
		String resourceFileName = classDir.getAbsolutePath() + File.separator + className.replace('.', '/') + ".class";
		File resourceFile = new File(resourceFileName);
		if(resourceFile.exists()){
			FileInputStream is = new FileInputStream(resourceFile);
			return is;
		}else{
			InputStream is = this.getClass().getClassLoader().getResourceAsStream(className);
			return is;
		}
	}
}
