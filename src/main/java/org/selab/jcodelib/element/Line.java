package org.selab.jcodelib.element;

public class Line {
	public int lineNum;
	public String code;
	public String codeNorm;
	public int fileMatcth = 0;
	public int packageMatch = 0;
	public int versionMatch = 0;

	public Line(int lineNum, String code) {
		super();
		this.lineNum = lineNum;
		this.code = code;
	}

	@Override
	public boolean equals(Object obj){
		if(obj instanceof Line){
			Line g = (Line)obj;
			return g.code.equals(this.code);
		}else if(obj instanceof String){
			return this.code.equals(obj);
		}
		return false;
	}
}
