package org.selab.jcodelib.element;

import java.util.HashMap;
import java.util.Map;

public class UnifiedHunk {
	public String oldFileName;
	public String newFileName;
	public int oldLineNum;
	public int oldLength;
	public int newLineNum;
	public int newLength;
	public Map<Integer, String> addedLines;
	public Map<Integer, String> deletedLines;

	public UnifiedHunk(int oldLineNum, int oldLength, int newLineNum, int newLength) {
		super();
		this.oldFileName = null;
		this.newFileName = null;
		this.oldLineNum = oldLineNum;
		this.oldLength = oldLength;
		this.newLineNum = newLineNum;
		this.newLength = newLength;
		this.addedLines = new HashMap<>();
		this.deletedLines = new HashMap<>();
	}

	public boolean isJava(){
		return !(oldFileName != null && newFileName != null
				&& (!oldFileName.toLowerCase().endsWith(".java")
						|| !newFileName.toLowerCase().endsWith(".java")));
	}

	public boolean isChanged(){
		return oldFileName != null && newFileName != null && oldFileName.equals(newFileName);
	}
}
