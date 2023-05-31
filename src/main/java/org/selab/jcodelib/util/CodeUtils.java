package org.selab.jcodelib.util;

import java.io.File;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.eclipse.jdt.core.JavaCore;
import org.eclipse.jdt.core.dom.AST;
import org.eclipse.jdt.core.dom.ASTNode;
import org.eclipse.jdt.core.dom.ASTParser;
import org.eclipse.jdt.core.dom.Comment;
import org.eclipse.jdt.core.dom.CompilationUnit;

import file.FileIOManager;
import org.selab.jcodelib.element.Hunk;
import org.selab.jcodelib.element.Line;
import org.selab.jcodelib.element.NormalizedHunk;

public class CodeUtils {

	public static CompilationUnit getCompilationUnit(String unitName, String[] classPath, String[] sourcePath, String source){
		ASTParser parser = ASTParser.newParser(AST.JLS8);
		parser.setKind(ASTParser.K_COMPILATION_UNIT);
		Map options = JavaCore.getOptions();
		JavaCore.setComplianceOptions(JavaCore.VERSION_1_8, options);
		parser.setCompilerOptions(options);
		parser.setEnvironment(classPath, sourcePath, null, true);
		parser.setUnitName(unitName);
		parser.setResolveBindings(true);
		parser.setSource(source.toCharArray());
		CompilationUnit cu = (CompilationUnit)parser.createAST(null);

		return cu;
	}

	public static CompilationUnit getCompilationUnit(String source) {
		ASTParser parser = ASTParser.newParser(AST.JLS8);
		parser.setKind(ASTParser.K_COMPILATION_UNIT);
		Map options = JavaCore.getOptions();
		JavaCore.setComplianceOptions(JavaCore.VERSION_1_8, options);
		parser.setCompilerOptions(options);
		parser.setSource(source.toCharArray());
		CompilationUnit cu = (CompilationUnit)parser.createAST(null);

		return cu;
	}

	public static String getTypeName(int type){
		return type == -1 ? "root" : ASTNode.nodeClassForType(type).getSimpleName();
	}

	public static Set<Integer> getCommentLineNumbers(File javaFile){
		Set<Integer> commentLines = new HashSet<>();
		int startLine = 0;
		int endLine = 0;
		String[] lines = null;
		try {
			String source = FileIOManager.getContent(javaFile);
			lines = source.split("\\n|\\r\\n|\\r");
			CompilationUnit cu = getCompilationUnit(source);
			List<Comment> comments = cu.getCommentList();
			for(Comment comment : comments){
				//For block comments or Javadocs, all lines should be in comments.
				if (comment.getNodeType() == ASTNode.BLOCK_COMMENT
						|| comment.getNodeType() == ASTNode.JAVADOC) {
					startLine = cu.getLineNumber(comment.getStartPosition());
					endLine = cu.getLineNumber(comment.getStartPosition()
							+ comment.getLength());
					//For block comment, check whether the first line has only the comment.
					if ((comment.getNodeType() == ASTNode.BLOCK_COMMENT && lines[startLine-1].trim().startsWith("/*"))
							|| comment.getNodeType() == ASTNode.JAVADOC) {
						commentLines.add(startLine);
					}
					for (int i = startLine + 1; i <= endLine; i++) {
						commentLines.add(i);
					}
				}else if(comment.getNodeType() == ASTNode.LINE_COMMENT){
					//For line comment, check whether that line starts with '//'
					startLine = cu.getLineNumber(comment.getStartPosition());
					if(lines[startLine-1].trim().startsWith("//")){
						commentLines.add(startLine);
					}
				}
			}

		} catch (Exception e) {
			e.printStackTrace();
			System.out.println("File:"+javaFile.getAbsolutePath());
			System.out.println("Line:"+startLine);
			System.out.println("Total Line:"+lines.length);
		}

		return commentLines;
	}

	public static List<Line> stripComments(String[] codeLines, Set<Integer> commentLineNumbers) {
		List<Line> lines = new ArrayList<>();
		for (int lineNum = 1; lineNum <= codeLines.length; lineNum++) {
			//if there exists a non-comment line, add a graft for the line.
			if (!commentLineNumbers.contains(lineNum)){
				if(codeLines[lineNum-1].trim().length() > 0){
					Line line = new Line(lineNum, codeLines[lineNum-1].trim());
					lines.add(line);
				}
			}
		}
		//Remove all the other comments included in grafts.
		for(Line line : lines){
			if(line.code.indexOf("/*") >= 0){
				line.code = line.code.substring(0, line.code.indexOf("/*"));
			}else if(line.code.indexOf("//") >= 0){
				line.code = line.code.substring(0, line.code.indexOf("//"));
			}
		}

		return lines;
	}

	public static NormalizedHunk stripComments(Hunk hunk, Set<Integer> oldCommentLineNumbers, Set<Integer> newCommentLineNumbers) {
		NormalizedHunk newHunk = new NormalizedHunk(hunk);
		if(hunk.type.equals("a")
				|| hunk.type.equals("c")) {
			String[] addedLines = hunk.addedCode.split("\n", hunk.newEndLine-hunk.newStartLine+1);
			for (int lineNum = hunk.newStartLine; lineNum <= hunk.newEndLine; lineNum++) {
				//if there exists a non-comment line, add a graft for the line.
				if (!newCommentLineNumbers.contains(lineNum)){
					if(addedLines[lineNum-hunk.newStartLine].trim().length() > 0){
						Line line = new Line(lineNum, addedLines[lineNum-hunk.newStartLine].trim());
						newHunk.insertedLines.add(line);
					}
				}
			}
		}

		//Remove all the other comments included in grafts.
		for(Line line : newHunk.insertedLines){
			if(line.code.indexOf("/*") >= 0){
				line.code = line.code.substring(0, line.code.indexOf("/*"));
			}else if(line.code.indexOf("//") >= 0){
				line.code = line.code.substring(0, line.code.indexOf("//"));
			}
		}

		if(hunk.type.equals("d")
				|| hunk.type.equals("c")) {
			String[] deletedLines = hunk.deletedCode.split("\n", hunk.endLine-hunk.startLine+1);
			for (int lineNum = hunk.startLine; lineNum <= hunk.endLine; lineNum++) {
				//if there exists a non-comment line, add a graft for the line.
				if (!oldCommentLineNumbers.contains(lineNum)){
					if(deletedLines[lineNum-hunk.startLine].trim().length() > 0){
						Line line = new Line(lineNum, deletedLines[lineNum-hunk.startLine].trim());
						newHunk.deletedLines.add(line);
					}
				}
			}
		}

		//Remove all the other comments included in grafts.
		for(Line line : newHunk.deletedLines){
			if(line.code.indexOf("/*") >= 0){
				line.code = line.code.substring(0, line.code.indexOf("/*"));
			}else if(line.code.indexOf("//") >= 0){
				line.code = line.code.substring(0, line.code.indexOf("//"));
			}
		}

		return newHunk;
	}

	public static NormalizedHunk normalizeHunk(Hunk hunk, Set<Integer> oldCommentLineNumbers, Set<Integer> newCommentLineNumbers){
		NormalizedHunk newHunk = CodeUtils.stripComments(hunk, oldCommentLineNumbers, newCommentLineNumbers);
		newHunk.insertedLines = normalizeLines(newHunk.insertedLines);
		newHunk.deletedLines = normalizeLines(newHunk.deletedLines);

		return newHunk;
	}

	public static NormalizedHunk normalizeHunk(NormalizedHunk hunk){
		NormalizedHunk newHunk = new NormalizedHunk(hunk);
		newHunk.insertedLines.addAll(normalizeLines(hunk.insertedLines));
		newHunk.deletedLines.addAll(normalizeLines(hunk.deletedLines));

		return newHunk;
	}

	public static List<Line> normalizeLines(List<Line> lines) {
		List<Line> normalized = new ArrayList<>();
		Line normalizedLine = null;
		for(Line line : lines){
			normalizedLine = normalizeLine(line);
			if(normalizedLine != null)
				normalized.add(normalizedLine);
		}
		return normalized;
	}

	public static Line normalizeLine(Line line) {
		String[] tokens = line.code.split("(?!\\\\)\\\"");
		StringBuffer sb = new StringBuffer();
		for(int i=0; i<tokens.length; i++){
			if(i%2==0){
				String token = tokens[i];
				token = token.replaceAll("[{};]", "");
				token = token.replaceAll("([\\[\\]\\(\\)\\|\\+\\*&%\\^\\$\\#\\@\\-\\:\\,\\/\\=]+)([\\s]+)", "$1");
				token = token.replaceAll("([\\s]+)([\\[\\]\\(\\)\\|\\+\\*\\&\\%\\^\\$\\#\\@\\-\\,\\:\\/\\=]+)", "$2");
				sb.append("\"");
				sb.append(token);
			}else{
				sb.append("\"");
				sb.append(tokens[i]);
			}
		}

		Line normalized = new Line(line.lineNum, sb.toString().substring(1).trim());
		if(normalized.code.length() > 0){
			return normalized;
		}else{
			return null;
		}
	}

}
