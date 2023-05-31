package org.selab.jcodelib.diffutil;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.selab.jcodelib.element.Hunk;
import org.selab.jcodelib.element.UnifiedHunk;

public class DiffParser {

	private static Pattern diffHunkInfo = Pattern
			.compile("^(\\d+)(,\\d+)?([acd]{1})(\\d+)(,\\d+)?");

	private static Pattern unifiedDiffHunkInfo = Pattern
			.compile("^@@\\s+-(?:(\\d+)(?:,(\\d+))?)\\s+\\+(?:(\\d+)(?:,(\\d+))?)\\s+@@");

	public static List<Hunk> parseDiff(String diff){
		List<Hunk> hunks = new ArrayList<Hunk>();
		String[] lines = diff.split("\n");
		Matcher m = null;
		Hunk hunk = null;
		String type;
		int startLine = 0;
		int endLine = 0;
		int newStartLine = 0;
		int newEndLine = 0;
		StringBuffer sb = null;

		for(int index=0; index<lines.length; index++){
			m = diffHunkInfo.matcher(lines[index]);
			if(m.find()){
				//Parse header.
				type = m.group(3);
				startLine = Integer.parseInt(m.group(1));
				endLine = m.group(2) == null ? startLine : Integer.parseInt(m.group(2).substring(1));
				newStartLine = Integer.parseInt(m.group(4));
				newEndLine = m.group(5) == null ? newStartLine : Integer.parseInt(m.group(5).substring(1));
				hunk = new Hunk(type, startLine, endLine, newStartLine, newEndLine);
				//Iterate deleted lines.
				if(type.equals("c") || type.equals("d")){
					sb = new StringBuffer();
					for(int i=startLine; i<=endLine; i++){
						index++;
						sb.append(lines[index].substring(2));	//remove "< "
						sb.append("\n");
					}
					hunk.deletedCode = sb.toString();
				}
				if(type.equals("c")){
					//skip ---
					index++;
				}
				if(type.equals("c") || type.equals("a")){
					sb = new StringBuffer();
					for(int i=newStartLine; i<=newEndLine; i++){
						index++;
						sb.append(lines[index].substring(2));	//remove "> "
						sb.append("\n");
					}
					hunk.addedCode = sb.toString();
				}
				//Check whether it is valid change.
				if(checkHunk(hunk)){
					hunks.add(hunk);
				}
			}
		}

		return hunks;
	}

	private static boolean checkHunk(Hunk hunk) {
		if(hunk.type.equals("d")){
			return hunk.deletedCode.trim().length() > 0;
		}else if(hunk.type.equals("a")){
			return hunk.addedCode.trim().length() > 0;
		}else if(hunk.type.equals("c")){
			return (hunk.addedCode.trim().length() > 0)
					|| (hunk.deletedCode.trim().length() > 0);
		}
		return false;
	}

	public static List<UnifiedHunk> parseUnitifedDiff(String diff){
		List<UnifiedHunk> hunks = new ArrayList<>();
		String[] lines = diff.split("\\n");
		int addedLineNum = 0;
		int deletedLineNum = 0;
		int addedLineCount = 0;
		int deletedLineCount = 0;
		String oldFileName = null;
		String newFileName = null;
		boolean parse = false;
		boolean isJava = false;
		UnifiedHunk hunk = null;

		for(String line : lines){
			if(parse){
				if(line.startsWith("+")){
					hunk.addedLines.put(addedLineNum, line.substring(1).trim());
					addedLineNum++;
					addedLineCount++;
				}else if(line.startsWith("-")){
					hunk.deletedLines.put(deletedLineNum, line.substring(1).trim());
					deletedLineNum++;
					deletedLineCount++;
				}else{
					addedLineNum++;
					deletedLineNum++;
				}
				if(addedLineCount == hunk.newLength &&
						deletedLineCount == hunk.oldLength){
					parse = false;
				}
			}

			if(line.startsWith("---")){
				oldFileName = getFileName(line);
				parse = false;
				addedLineNum = 0;
				deletedLineNum = 0;
			}
			if(line.startsWith("+++")){
				newFileName = getFileName(line);
				//Ignore non-java files.
				if ((oldFileName != null
						&& !oldFileName.endsWith(".java"))
						|| (newFileName != null
						&& !newFileName.endsWith(".java"))) {
					isJava = false;
				} else {
					isJava = true;
				}
			}
			if(line.startsWith("@@")){
				if(isJava){
					if(hunk != null)
						hunks.add(hunk);
					parse = true;
					hunk = parseHunkInfo(line);
					hunk.oldFileName = oldFileName;
					hunk.newFileName = newFileName;
					addedLineNum = hunk.newLineNum;
					deletedLineNum = hunk.oldLineNum;
				}
			}
		}

		return hunks;
	}

	public static UnifiedHunk parseHunkInfo(String line) {
		UnifiedHunk hunk = null;
		Matcher m = unifiedDiffHunkInfo.matcher(line);
		if(m.find()){
			String oldLN = m.group(1) == null ? "0" : m.group(1);
			String oldLen = m.group(2) == null ? "0" : m.group(2);
			String newLN = m.group(3) == null ? "0" : m.group(3);
			String newLen = m.group(4) == null ? "0" : m.group(4);
			hunk = new UnifiedHunk(Integer.parseInt(oldLN),
					Integer.parseInt(oldLen),
					Integer.parseInt(newLN),
					Integer.parseInt(newLen));
		}
		return hunk;
	}

	public static List<String> getGitChangedFiles(String diff){
		List<String> files = new ArrayList<String>();
		String[] lines = diff.split("\\n");
		String oldFileName = null;
		String newFileName = null;
		for(String line : lines){
			if(line.startsWith("---")){
				oldFileName = getFileName(line);
			}else if(line.startsWith("+++")){
				newFileName = getFileName(line);
				if(oldFileName != null &&
						newFileName != null &&
						oldFileName.equals(newFileName)){
					files.add(newFileName);
				}
			}
		}
		return files;

	}

	public static List<String> getGitChangedJavaFiles(String diff){
		List<String> javaFiles = new ArrayList<String>();
		List<String> files = getGitChangedFiles(diff);
		for(String f : files){
			if(f.endsWith(".java"))
				javaFiles.add(f);
		}
		return javaFiles;

	}

	private static String getFileName(String line) {
		String fileName = line.trim().substring(line.indexOf("/")+1);
		if(fileName.equals("dev/null")){
			fileName = null;
		}
		return fileName;
	}
}
