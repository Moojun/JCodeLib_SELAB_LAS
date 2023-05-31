package org.selab.jcodelib.util;

import java.io.File;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.eclipse.jdt.core.compiler.CharOperation;
import org.eclipse.jdt.core.compiler.IProblem;
import org.eclipse.jdt.core.dom.CompilationUnit;
import org.eclipse.jdt.internal.compiler.ClassFile;

import file.FileIOManager;
import org.selab.jcodelib.compiler.CompilationUnitImpl;
import org.selab.jcodelib.compiler.Compiler;
import org.selab.jcodelib.compiler.CustomClassLoader;

public class CompileUtils {

	public static String getPathEntryString(String[] pathEntries){
		StringBuffer sb = new StringBuffer();
		if (pathEntries.length > 0) {
			sb.append(pathEntries[0]);
			for(int i=1; i<pathEntries.length; i++){
				sb.append(":");
				sb.append(pathEntries[i]);
			}
		}
		return sb.toString();
	}

	public static void compile(String fileName, String javaVersion, String[] classPath, String[] sourcePath) throws Exception {
		List<String> classes = new ArrayList<>();
		File file = new File(fileName);
		String source = FileIOManager.getContent(file);
		CompilationUnit cu = CodeUtils.getCompilationUnit(file.getName(), classPath, sourcePath, source);
		Compiler compiler = new Compiler();
		String classPathEntries = getPathEntryString(classPath);
		URL[] urls = getUrls(classPathEntries);
		CustomClassLoader loader = new CustomClassLoader(urls, compiler.getClass()
				.getClassLoader(), compiler.classes);
		CompilationUnitImpl cui = new CompilationUnitImpl(cu);
		String className = CharOperation.toString(cui.getPackageName())
				+ "." + new String(cui.getMainTypeName());
		File tmpDir = new File("tmp");
		if(!tmpDir.exists()){
			tmpDir.mkdir();
		}
		String path = tmpDir.getAbsolutePath()+File.separator;
		boolean error = compiler.compile(loader, source,
				cu, path, true, javaVersion);
		if(error){
			for(IProblem problem : compiler.problems){
				if (problem.isError()) {
					StringBuffer buffer = new StringBuffer();
					buffer.append(problem.getMessage());
					buffer.append(" line:");
					buffer.append(problem.getSourceLineNumber());
					System.out.println(buffer.toString());
				}
			}
			throw new Exception("Compile Error");
		}else{
			//Add tmp to class path.
			List<URL> urlList = new ArrayList<URL>();
			urlList.addAll(Arrays.asList(urls));
			urlList.add(tmpDir.toURI().toURL());
			urls = new URL[urlList.size()];
			urlList.toArray(urls);

			loader = new CustomClassLoader(urls, compiler.getClass()
					.getClassLoader(), compiler.classes);

			//Get all classes (including inner classes)
			for(ClassFile cf : compiler.classes){
				className = CharOperation.toString(cf.getCompoundName());
				classes.add(className);
				loader.loadClass(className);
			}
		}
	}

	private static URL[] getUrls(String classPathEntries) throws MalformedURLException {
		String[] classPaths = classPathEntries.split(":");
		List<URL> urlList = new ArrayList<URL>();
		for(String classPath : classPaths){
			if(classPath.toLowerCase().endsWith(".jar")){
				urlList.add(new URL("jar:file://"+classPath+"!/"));
			}else{
				urlList.add(new URL("file://"+classPath+"/"));
			}
		}
		URL[] urls = new URL[urlList.size()];
		urlList.toArray(urls);
		return urls;
	}

}
