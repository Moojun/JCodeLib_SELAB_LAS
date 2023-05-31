package org.selab.jcodelib.jgit;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.eclipse.jgit.api.Git;
import org.eclipse.jgit.api.ResetCommand.ResetType;
import org.eclipse.jgit.api.errors.CheckoutConflictException;
import org.eclipse.jgit.api.errors.GitAPIException;
import org.eclipse.jgit.api.errors.NoHeadException;
import org.eclipse.jgit.diff.DiffEntry;
import org.eclipse.jgit.diff.DiffFormatter;
import org.eclipse.jgit.errors.IncorrectObjectTypeException;
import org.eclipse.jgit.errors.MissingObjectException;
import org.eclipse.jgit.lib.ObjectId;
import org.eclipse.jgit.lib.ObjectReader;
import org.eclipse.jgit.lib.Repository;
import org.eclipse.jgit.revwalk.RevCommit;
import org.eclipse.jgit.revwalk.RevTree;
import org.eclipse.jgit.revwalk.RevWalk;
import org.eclipse.jgit.storage.file.FileRepositoryBuilder;
import org.eclipse.jgit.treewalk.AbstractTreeIterator;
import org.eclipse.jgit.treewalk.CanonicalTreeParser;

public class ReposHandler {

	public static Repository getRepository(String reposPath) {
		Repository repo = null;
		try {
			FileRepositoryBuilder builder = new FileRepositoryBuilder();
			repo = builder.setGitDir(new File(reposPath)).readEnvironment().findGitDir().build();
		} catch (IOException e) {
			e.printStackTrace();
		}

		return repo;
	}

	public static AbstractTreeIterator getTreeParser(Repository repository, String objectId)
			throws IOException, MissingObjectException, IncorrectObjectTypeException {
		RevWalk walk = new RevWalk(repository);
		RevCommit commit = walk.parseCommit(ObjectId.fromString(objectId));
		RevTree tree = walk.parseTree(commit.getTree().getId());

		CanonicalTreeParser treeParser = new CanonicalTreeParser();
		ObjectReader reader = repository.newObjectReader();
		try {
			treeParser.reset(reader, tree.getId());
		} finally {
			reader.close();
		}

		walk.close();

		return treeParser;
	}

	public static Iterable<RevCommit> getAllCommits(Git git) throws IOException, NoHeadException, GitAPIException {
		Iterable<RevCommit> commits = git.log().all().call();
		return commits;
	}

	public static List<String> getAllCommitIds(Git git) throws IOException, NoHeadException, GitAPIException {
		Iterable<RevCommit> commits = git.log().all().call();
		List<String> commitIds = new ArrayList<String>();
		for (RevCommit commit : commits) {
			commitIds.add(commit.getId().getName());
		}
		// Start from the base revision ~ HEAD.
		Collections.reverse(commitIds);

		return commitIds;
	}

	public static String getDiff(Repository repos, String oldCommitId, String newCommitId)
			throws MissingObjectException, IncorrectObjectTypeException, IOException, GitAPIException {
		return getDiff(repos, oldCommitId, newCommitId, false);
	}

	public static String getDiff(Repository repos, String oldCommitId, String newCommitId, boolean nameOnly)
			throws MissingObjectException, IncorrectObjectTypeException, IOException, GitAPIException {
		AbstractTreeIterator oldTreeParser = getTreeParser(repos, oldCommitId);
		AbstractTreeIterator newTreeParser = getTreeParser(repos, newCommitId);
		Git git = new Git(repos);
		List<DiffEntry> diff = git.diff().setOldTree(oldTreeParser).setNewTree(newTreeParser)
				.setShowNameAndStatusOnly(nameOnly).call();
		ByteArrayOutputStream os = null;
		String strDiff = null;
		try {
			os = new ByteArrayOutputStream();
			DiffFormatter formatter = new DiffFormatter(os);
			formatter.setRepository(repos);
			formatter.format(diff);
			strDiff = os.toString();
			formatter.close();
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			try {
				if (os != null) {
					os.close();
				}
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		git.close();

		return strDiff;
	}

	public static void update(Git git, String commitId) throws CheckoutConflictException, GitAPIException {
		git.reset().setMode(ResetType.HARD).setRef(commitId).call();
	}

	public static void update(File reposDir, String commitId) throws IOException, InterruptedException {
		Runtime run = Runtime.getRuntime();
		Process p = run.exec("git reset --hard "+commitId, null, reposDir);
		p.waitFor();
	}

	public static List<String> getChangedJavaFiles(Repository repos, String oldCommitId, String newCommitId)
			throws MissingObjectException, IncorrectObjectTypeException, IOException, GitAPIException {
		List<String> changedJavaFiles = new ArrayList<String>();

		//Get name and status.
		AbstractTreeIterator oldTreeParser = getTreeParser(repos, oldCommitId);
		AbstractTreeIterator newTreeParser = getTreeParser(repos, newCommitId);
		Git git = new Git(repos);
		List<DiffEntry> diff = git.diff()
				.setShowNameAndStatusOnly(true)
				.setOldTree(oldTreeParser)
				.setNewTree(newTreeParser)
				.call();
		ByteArrayOutputStream os = null;
		String nameAndStatus = null;
		try {
			os = new ByteArrayOutputStream();
			DiffFormatter formatter = new DiffFormatter(os);
			formatter.setRepository(repos);
			formatter.format(diff);
			nameAndStatus = os.toString();
			formatter.close();
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			try {
				if (os != null) {
					os.close();
				}
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		git.close();

		//Parse and collect java file paths.
		if(nameAndStatus != null){
			String[] lines = nameAndStatus.split("\\n");
			System.out.println(nameAndStatus);
			for(String line : lines){
				if(line.startsWith("M")
						&& line.endsWith(".java")){
					changedJavaFiles.add(line.substring(1).trim());
				}
			}
		}

		return changedJavaFiles;
	}
}
