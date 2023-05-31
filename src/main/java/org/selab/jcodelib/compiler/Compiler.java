package org.selab.jcodelib.compiler;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import org.eclipse.jdt.core.compiler.CharOperation;
import org.eclipse.jdt.core.compiler.CompilationProgress;
import org.eclipse.jdt.core.compiler.IProblem;
import org.eclipse.jdt.core.dom.CompilationUnit;
import org.eclipse.jdt.internal.compiler.ClassFile;
import org.eclipse.jdt.internal.compiler.DefaultErrorHandlingPolicies;
import org.eclipse.jdt.internal.compiler.env.ICompilationUnit;
import org.eclipse.jdt.internal.compiler.impl.CompilerOptions;
import org.eclipse.jdt.internal.compiler.problem.DefaultProblemFactory;

public class Compiler {

	public List<ClassFile> classes;
	public List<IProblem> problems;
	public String className;

	public Compiler(){
		this.problems = new ArrayList<IProblem>();
		this.classes = new ArrayList<ClassFile>();
	}

	public boolean compile(String source, CompilationUnit unit, String path, boolean writeDown, String version) throws IOException, FileNotFoundException{
		return compile(getClass().getClassLoader(), source, unit, path, writeDown, version);
	}

	public boolean compile(ClassLoader loader, String source, CompilationUnit unit, String path, boolean writeDown, String version) throws IOException, FileNotFoundException{

		CompilationUnitImpl cu = new CompilationUnitImpl(unit);
		org.eclipse.jdt.internal.compiler.batch.CompilationUnit newUnit =
				new org.eclipse.jdt.internal.compiler.batch.CompilationUnit(
						source.toCharArray(), new String(cu.getFileName()), "UTF8");

		this.className = CharOperation.toString(cu.getPackageName()) + "." + new String(cu.getMainTypeName());

		CompilationProgress progress = null;
		CompilerRequestorImpl requestor = new CompilerRequestorImpl();
		CompilerOptions options = new CompilerOptions();
		Map<String,String> optionsMap = new HashMap<String, String>();
		optionsMap.put(CompilerOptions.OPTION_Compliance, version);
		optionsMap.put(CompilerOptions.OPTION_Source, version);
		optionsMap.put(CompilerOptions.OPTION_LineNumberAttribute,CompilerOptions.GENERATE);
		optionsMap.put(CompilerOptions.OPTION_SourceFileAttribute,CompilerOptions.GENERATE);
		options.set(optionsMap);

		org.eclipse.jdt.internal.compiler.Compiler compiler =
				new org.eclipse.jdt.internal.compiler.Compiler(new NameEnvironmentImpl(loader, newUnit),
						DefaultErrorHandlingPolicies.proceedWithAllProblems(),
						options,requestor,new DefaultProblemFactory(Locale.getDefault()),
						null, progress);
		compiler.compile(new ICompilationUnit[]{ newUnit });

		this.classes = requestor.getClasses();
		this.problems = requestor.getProblems();

		boolean error = false;
		for (Iterator<IProblem> it = problems.iterator(); it.hasNext();) {
			IProblem problem = it.next();
			if(problem.isError())
				error = true;
		}

		if (writeDown) {
			for (ClassFile cf : classes) {
				String filePath	= CharOperation.charToString(cf.fileName());
				String pakagePath = path + filePath.substring(0, filePath.lastIndexOf("/")+1);
				File pakageDir = new File(pakagePath);
				if(!pakageDir.exists()){
					pakageDir.mkdirs();
				}
				File f = new File(path + filePath + ".class");
				f.createNewFile();
				FileOutputStream fos = new FileOutputStream(f);
				fos.write(cf.getBytes());
				fos.flush();
				fos.close();
			}
		}

		return error;
	}
}
