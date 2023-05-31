package org.selab.jcodelib.coverage;

import java.util.SortedMap;
import java.util.TreeMap;

public class LineCoverage {
	public String fileName;
	public SortedMap<Integer, Integer> coverage;

	public LineCoverage(String fileName){
		this.fileName = fileName;
		coverage = new TreeMap<>();
	}

	public void addLine(int lineNum, int count){
		coverage.put(lineNum, coverage.containsKey(lineNum) ? coverage.get(lineNum)+count : count);
	}
}
