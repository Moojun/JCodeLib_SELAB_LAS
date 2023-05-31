package org.selab.jcodelib.element;

import java.io.Serializable;

public class Mutation implements Serializable {
	private static final long serialVersionUID = 6398765373091810328L;
	public String fileName;
	public Hunk hunk;
	public Mutation(String fileName, Hunk hunk) {
		super();
		this.fileName = fileName;
		this.hunk = hunk;
	}

	@Override
	public String toString(){
		StringBuffer sb = new StringBuffer();
		sb.append(fileName);
		sb.append("|");
		sb.append(hunk.type);
		sb.append("|");
		sb.append(hunk.startLine);
		sb.append("|");
		sb.append(hunk.endLine);
		sb.append("\n");
		sb.append(hunk.deletedCode);
		sb.append("---\n");
		sb.append(hunk.addedCode);

		return sb.toString();
	}
}
