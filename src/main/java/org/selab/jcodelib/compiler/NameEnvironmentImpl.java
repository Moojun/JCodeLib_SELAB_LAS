package org.selab.jcodelib.compiler;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;

import org.eclipse.jdt.core.compiler.CharOperation;
import org.eclipse.jdt.internal.compiler.batch.CompilationUnit;
import org.eclipse.jdt.internal.compiler.classfmt.ClassFileReader;
import org.eclipse.jdt.internal.compiler.classfmt.ClassFormatException;
import org.eclipse.jdt.internal.compiler.env.ICompilationUnit;
import org.eclipse.jdt.internal.compiler.env.INameEnvironment;
import org.eclipse.jdt.internal.compiler.env.NameEnvironmentAnswer;

public class NameEnvironmentImpl implements INameEnvironment {
	
	private ICompilationUnit cu;
	private String fullName;
	private ClassLoader loader = null;
	
	public NameEnvironmentImpl(CompilationUnit cu){
		this.cu = cu;
		this.fullName = CharOperation.toString(this.cu.getPackageName()) + "." +
		new String(this.cu.getMainTypeName());
	}
	
	public NameEnvironmentImpl(ClassLoader loader, CompilationUnit cu){
		this.cu = cu;
		this.fullName = CharOperation.toString(this.cu.getPackageName()) + "." +
		new String(this.cu.getMainTypeName());
		this.loader = loader;
	}

	public NameEnvironmentAnswer findType(char[][] compoundTypeName) {
		// TODO Auto-generated method stub
		return findType(CharOperation.toString(compoundTypeName));
	}

	public NameEnvironmentAnswer findType(char[] typeName, char[][] packageName) {
		// TODO Auto-generated method stub
		String fullName = CharOperation.toString(packageName);
		if (typeName != null) {
			if (fullName.length() > 0)
				fullName += ".";
			
			fullName += new String(typeName); 
		}
		return findType(fullName);
	}	

	public boolean isPackage(char[][] parentPackageName, char[] packageName) {
		// TODO Auto-generated method stub
		String fullName = CharOperation.toString(parentPackageName);
		if (packageName != null) {
			if (fullName.length() > 0)
				fullName += ".";
			
			fullName += new String(packageName); 
		}
		if (findType(fullName) != null)
			return false;		
				
		try{
			if(this.loader != null){
				return (this.loader.loadClass(fullName) == null);
			}else{
				return (getClass().getClassLoader().loadClass(fullName) == null);
			}
		}
		catch(ClassNotFoundException e) {
			return true;
		}
	}

	public void cleanup() {
		// TODO Auto-generated method stub

	}
	
	private NameEnvironmentAnswer findType(String fullName) {
		if (this.fullName.equals(fullName))
			return new NameEnvironmentAnswer(cu, null);
		
		try {
			InputStream is = this.loader != null ? 
					this.loader.getResourceAsStream(fullName.replace('.', '/') + ".class") :
						getClass().getClassLoader().getResourceAsStream(fullName.replace('.', '/') + ".class") 	;
			if (is != null) { 
				byte[] buffer = new byte[8192];
				int bytes = 0;
				ByteArrayOutputStream os = new ByteArrayOutputStream(buffer.length);
				while ((bytes = is.read(buffer, 0, buffer.length)) > 0) 
					os.write(buffer, 0, bytes);
				
				os.flush();
				ClassFileReader classFileReader = new ClassFileReader(os.toByteArray(),fullName.toCharArray(),true);
				return new NameEnvironmentAnswer(classFileReader, null);
			}
			return null;
		}
		catch (IOException e) {
			throw new RuntimeException(e);
		}
		catch (ClassFormatException e) {
			throw new RuntimeException(e);
		}		
	}

}
