package org.selab.jcodelib.element;

import java.io.Serializable;

public class Hunk implements Serializable {
	private static final long serialVersionUID = 1631846432639100238L;
	public String type;	//c:change, a:add, d:delete
	public int startLine;
	public int endLine;
	public int newStartLine;
	public int newEndLine;
	public String deletedCode;
	public String addedCode;

	public Hunk(String type, int startLine, int endLine, int newStartLine,
			int newEndLine) {
		super();
		this.type = type;
		this.startLine = startLine;
		this.endLine = endLine;
		this.newStartLine = newStartLine;
		this.newEndLine = newEndLine;
		this.deletedCode = "";
		this.addedCode = "";
	}


	public int size(){
		int oldLines = endLine - startLine + 1;
		int newLines = newEndLine - newStartLine + 1;
		if("c".equals(type)){
			return oldLines > newLines ? oldLines : newLines;
		}else if("a".equals(type)){
			return newLines;
		}else{
			return oldLines;
		}
	}
}
