package org.selab.jcodelib.diffutil;

import java.util.HashMap;
import java.util.Map;

import kr.ac.seoultech.selab.esscore.model.Script;
import script.model.EditScript;

public class DiffResult {

	private Object script;
	private long runtime;
	private Map<String, Object> metaInfo;
	private static final String EXACT_MATCH = "exact";
	private static final String EXACT_MATCH_COUNT = "exact_count";
	private static final String SIMILAR_MATCH = "similar";
	private static final String FOLLOWUP_MATCH = "follow";
	private static final String LEAF_MATCH = "leaf";

	public DiffResult() {
		this(new Script(), 0);
	}

	public DiffResult(Object script) {
		this(script, 0);
	}

	public DiffResult(Object script, long runtime) {
		super();
		this.script = script;
		this.runtime = runtime;
		this.metaInfo = new HashMap<>();
	}

	public Object getScript() {
		return script;
	}

	public void setScript(Object script) {
		this.script = script;
	}

	public long getRuntime() {
		return runtime;
	}

	public void setRuntime(long runtime) {
		this.runtime = runtime;
	}

	public Map<String, Object> getMetaInfo(String key) {
		return metaInfo;
	}

	public int getExactMatch() {
		return (int)metaInfo.getOrDefault(DiffResult.EXACT_MATCH, 0);
	}

	public int getSimilarMatch() {
		return (int)metaInfo.getOrDefault(DiffResult.SIMILAR_MATCH, 0);
	}

	public int getFollowUpMatch() {
		return (int)metaInfo.getOrDefault(DiffResult.FOLLOWUP_MATCH, 0);
	}

	public int getLeafMatch() {
		return (int)metaInfo.getOrDefault(DiffResult.LEAF_MATCH, 0);
	}

	public int getExactMatchCount() {
		return (int)metaInfo.getOrDefault(DiffResult.EXACT_MATCH_COUNT, 0);
	}

	public void updateMatchCount(EditScript script) {
		metaInfo.put(DiffResult.EXACT_MATCH, script.exactMatch);
		metaInfo.put(DiffResult.SIMILAR_MATCH, script.similarMatch);
		metaInfo.put(DiffResult.FOLLOWUP_MATCH, script.followupMatch);
		metaInfo.put(DiffResult.LEAF_MATCH, script.leafMatch);
		metaInfo.put(DiffResult.EXACT_MATCH_COUNT, script.exactMatchCount);
	}

}
