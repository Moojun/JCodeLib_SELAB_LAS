package org.selab.jcodelib.element;

import java.util.ArrayList;
import java.util.List;

public class NormalizedHunk {
	public String type;	//c:change, a:add, d:delete
	public int startLine;
	public int newStartLine;
	public List<Line> insertedLines;
	public List<Line> deletedLines;

	public NormalizedHunk(String type, int startLine, int newStartLine) {
		super();
		this.type = type;
		this.startLine = startLine;
		this.newStartLine = newStartLine;
		this.insertedLines = new ArrayList<Line>();
		this.deletedLines = new ArrayList<Line>();
	}

	public NormalizedHunk(String type, int startLine, int newStartLine, List<Line> insertedLines, List<Line> deletedLines) {
		super();
		this.type = type;
		this.startLine = startLine;
		this.newStartLine = newStartLine;
		this.insertedLines = new ArrayList<Line>(insertedLines);
		this.deletedLines = new ArrayList<Line>(deletedLines);
	}

	public NormalizedHunk(Hunk hunk){
		this(hunk.type, hunk.startLine, hunk.newStartLine);
	}

	public NormalizedHunk(NormalizedHunk hunk){
		this(hunk.type, hunk.startLine, hunk.newStartLine);
	}

	public int size(){
		if("c".equals(type)){
			return insertedLines.size() > deletedLines.size() ? insertedLines.size() :deletedLines.size();
		}else if("a".equals(type)){
			return insertedLines.size();
		}else{
			return deletedLines.size();
		}
	}
}
