package org.selab;

import db.DBManager;
import file.FileIOManager;

import org.selab.jcodelib.diffutil.DiffResult;
import org.selab.jcodelib.diffutil.TreeDiff;
import org.selab.jcodelib.jgit.ReposHandler;
import kr.ac.seoultech.selab.esscore.model.ESNodeEdit;
import kr.ac.seoultech.selab.esscore.model.Script;

import java.io.*;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class Main {
    //Hard-coded projects - need to read it from DB.
    //public static String[] projects;
    public static String baseDir = "subjects";
    public static final String oldDir = "old";
    public static final String newDir = "new";
    public static long commitCountOfLAS = 1;
    public static String [] projectArr = {"elasticsearch", "ballerina-lang", "crate", "neo4j","sakai", "wildfly"};


    public static void main(String[] args) throws IOException {

        // Put SCD Tool here.
        String tool = "LAS";
        DBManager db = null;

        // use command line arguments
        String project = args[0];

        try {
            //Change db.properties.
            db = new DBManager("src/main/resources/db.properties");

            //Connect DB.
            Connection con = db.getConnection();

            // if exception occurs, do rollback
            con.setAutoCommit(false);

            // Collect and store LAS EditOp, runtime in DB.
            PreparedStatement psLAS = con.prepareStatement("insert into changes_LAS " +
                    " ( file_id, tool, ESNodeEdit, ESNodeEdit_type, ESNodeEdit_pos, " +
                    " ESNodeEdit_loc, ESNodeEdit_loc_type, ESNodeEdit_loc_pos, ESNodeEdit_loc_length, ESNodeEdit_loc_label, " +
                    " ESNodeEdit_node, ESNodeEdit_node_type, ESNodeEdit_node_pos, ESNodeEdit_node_length, ESNodeEdit_node_label ) " +
                    " values ( ?, ?, ?, ?, ?, " +
                    "?, ?, ?, ?, ?, " +
                    " ?, ?, ?, ?, ? )");

            PreparedStatement psLASrunTime = con.prepareStatement("insert into changes_LAS_runtime " +
                    " (file_id, runtime_gitReset, runtime_editScript, exact_match, similar_match," +
                    " followup_match, leaf_match, exact_match_count )" +
                    " values ( ?, ?, ?, ?, ?, " +
                    "?, ?, ?) " );

            System.out.println("Collecting Changes from " + project);
            String oldReposPath = String.join(File.separator, baseDir, oldDir, project) + File.separator;
            String newReposPath = String.join(File.separator, baseDir, newDir, project) + File.separator;
            File oldReposDir = new File(oldReposPath);
            File newReposDir = new File(newReposPath);

            // Prepare files.
            List<String> fileInfo = new ArrayList<>();

            PreparedStatement fileSel = con.prepareStatement(
                    " select c.commit_id commit_id, c.old_commit old_commit, c.new_commit new_commit, " +
                            " f.file_path file_path, f.file_id file_id " +
                            " from commits c, files f where c.commit_id = f.commit_id and c.project_name = '" + project + "'" +
                            " and c.merged_commit_status != 'T' " +
                            " order by file_id, commit_id ");
            ResultSet fileRS = fileSel.executeQuery();
            while (fileRS.next()) {
                String str = String.join(",",
                        String.valueOf(fileRS.getInt("commit_id")),
                        fileRS.getString("old_commit"),
                        fileRS.getString("new_commit"),
                        fileRS.getString("file_path"),
                        String.valueOf(fileRS.getInt("file_id")));
                fileInfo.add(str);
            }

            fileRS.close();
            fileSel.close();

            System.out.println("Total " + fileInfo.size() + " revisions.");

            for (int i = 0; i < fileInfo.size(); i++) {
                String key = fileInfo.get(i);
                String[] tokens = key.split(",");
                String commitId = tokens[0];
                String oldCommitId = tokens[1];
                String newCommitId = tokens[2];
                String filePath = tokens[3];
                String fileId = tokens[4];
                System.out.println("CommitId : " + commitId + ", fileId : " + fileId + ", oldCommitId : " + oldCommitId + ", newCommitId : " + newCommitId);

                // Reset hard to old/new commit IDs.
                long gitResetStartTime = System.currentTimeMillis();
                ReposHandler.update(oldReposDir, oldCommitId);
                ReposHandler.update(newReposDir, newCommitId);
                long gitResetFinishTime = System.currentTimeMillis();
                long gitResetElapsedTime = gitResetFinishTime - gitResetStartTime;

                try {
                    if (filePath.contains("/test/")) // ignore test code in GitHub project
                        continue;

                    List<String> projectList = new ArrayList<>(Arrays.asList(projectArr));
                    if (!filePath.contains("/org/") && projectList.contains(project))
                        continue;

                    File oldFile = new File(oldReposPath + filePath);
                    File newFile = new File(newReposPath + filePath);
                    String oldCode = "";
                    String newCode = "";

                    // Handle FileNotFoundException in oldFile, newFile
                    oldCode = FileIOManager.getContent(oldFile).intern();
                    newCode = FileIOManager.getContent(newFile).intern();

                    //Practically these files are deleted/inserted.
                    if (oldCode.length() == 0 || newCode.length() == 0) {
                        continue;
                    }

                    // Apply Source Code Differencing Tools: LAS
                    DiffResult diffResultOfLAS = null;
                    try {
                        diffResultOfLAS = TreeDiff.diffLAS(oldCode, newCode);
                    } catch (NullPointerException e) {
                        System.out.println("e = " + e);
                    } finally {
                        if (diffResultOfLAS != null) {
                            long runtimeOfLAS = diffResultOfLAS.getRuntime();
                            int exactMatchTime = diffResultOfLAS.getExactMatch();
                            int similarMatchTime = diffResultOfLAS.getSimilarMatch();
                            int followUpMatchTime = diffResultOfLAS.getFollowUpMatch();
                            int leafMatchTime = diffResultOfLAS.getLeafMatch();
                            int exactMathCount = diffResultOfLAS.getExactMatchCount();

                            Script scriptLAS = (Script) diffResultOfLAS.getScript();

                            for (ESNodeEdit esNodeEdit : scriptLAS.editOps) {
                                psLAS.clearParameters();
                                psLASrunTime.clearParameters();
                                psLAS.setInt(1, Integer.parseInt(fileId));         //file_id
                                psLAS.setString(2, tool);           //tool: GT, etc..

                                // ESNodeEdit
                                psLAS.setString(3, esNodeEdit.toString());
                                psLAS.setString(4, esNodeEdit.type);
                                psLAS.setInt(5, esNodeEdit.position);
                                // ESNodeEdit_loc
                                psLAS.setString(6, esNodeEdit.location.toString());
                                psLAS.setString(7, esNodeEdit.location.type);
                                psLAS.setInt(8, esNodeEdit.location.pos);
                                psLAS.setInt(9, esNodeEdit.location.length);
                                psLAS.setString(10, esNodeEdit.location.label);
                                // ESNodeEdit_node
                                psLAS.setString(11, esNodeEdit.node.toString());
                                psLAS.setString(12, esNodeEdit.node.type);
                                psLAS.setInt(13, esNodeEdit.node.pos);
                                psLAS.setInt(14, esNodeEdit.node.length);
                                psLAS.setString(15, esNodeEdit.node.label);

                                psLASrunTime.setInt(1, Integer.parseInt(fileId));         //file_id
                                psLASrunTime.setLong(2, gitResetElapsedTime);
                                psLASrunTime.setLong(3, runtimeOfLAS);
                                psLASrunTime.setInt(4, exactMatchTime);
                                psLASrunTime.setInt(5, similarMatchTime);
                                psLASrunTime.setInt(6, followUpMatchTime);
                                psLASrunTime.setInt(7, leafMatchTime);
                                psLASrunTime.setInt(8, exactMathCount);

                                psLAS.addBatch();
                                psLASrunTime.addBatch();
                                commitCountOfLAS++;
                            }
                        }
                    }

                    if (commitCountOfLAS % 100 == 0) {
                        psLAS.executeBatch();
                        psLASrunTime.executeBatch();
                        con.commit();
                        psLAS.clearBatch();
                        psLASrunTime.clearBatch();
                    }

                } catch (Exception e) {
                    e.printStackTrace();
                    try {
                        // If failed, do rollback
                        con.rollback();
                    } catch (SQLException E) {
                        E.printStackTrace();
                    }
                }

                // Committing for the rest of the syntax that has not been committed
                psLAS.executeBatch();
                psLASrunTime.executeBatch();
                con.commit();
                psLAS.clearBatch();
                psLASrunTime.clearBatch();
            }

            if (psLAS != null){psLAS.close(); psLAS = null;}
            if (psLASrunTime != null){psLASrunTime.close(); psLASrunTime = null;}
            if (con != null){con.close(); con = null;}
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            db.close();
        }
    }
}
