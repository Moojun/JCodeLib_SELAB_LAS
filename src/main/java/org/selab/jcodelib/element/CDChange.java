package org.selab.jcodelib.element;

public class CDChange {

	public static final String INSERT = "INS";
	public static final String DELETE = "DEL";
	public static final String MOVE = "MOV";
	public static final String UPDATE = "UPD";

	private String changeType;
	private String entityType;
	private int startPos;
	private int endPos;

	public CDChange(String changeType, String entityType, int startPos, int endPos) {
		super();
		this.changeType = changeType;
		this.entityType = entityType;
		this.startPos = startPos;
		this.endPos = endPos;
	}

	public String getChangeType() {
		return changeType;
	}

	public void setChangeType(String changeType) {
		this.changeType = changeType;
	}

	public String getEntityType() {
		return entityType;
	}

	public void setEntityType(String entityType) {
		this.entityType = entityType;
	}

	public int getStartPos() {
		return startPos;
	}

	public void setStartPos(int startPos) {
		this.startPos = startPos;
	}

	public int getEndPos() {
		return endPos;
	}

	public void setEndPos(int endPos) {
		this.endPos = endPos;
	}

	@Override
	public String toString() {
		return changeType + " " + entityType + "(" + startPos + "," + endPos +")";
	}
}
