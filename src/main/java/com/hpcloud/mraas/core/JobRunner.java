package com.hpcloud.mraas.core;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.lang.reflect.Array;
import java.lang.reflect.Method;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Enumeration;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;
import java.util.jar.Manifest;
import java.util.regex.Pattern;

import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JobRunner {

    private static final Logger logger = LoggerFactory.getLogger(JobRunner.class);

    public static final Pattern MATCH_ANY = Pattern.compile(".*");
        
    private File mapReduceJarFile; 
    private String[] jobArgs; 
    private File[] confFiles;
    private File tmpDir;
    
    public JobRunner(File mapReduceJarFile, 
                     String[] jobArgs, 
                     File[] confFiles,
                     File tmpDir) {
        this.mapReduceJarFile = mapReduceJarFile;
        this.jobArgs = jobArgs;
        this.confFiles = confFiles;
        this.tmpDir = tmpDir;        
    }

    
    public void runJar() throws Throwable {
        int currentArg = 0;
        String mainClassName = null;
        
        JarFile jarFile = null;
        try {
            jarFile = new JarFile(mapReduceJarFile);
        } catch (IOException e) {
            // exception does not have file name:
            throw new IOException("Error opening map-reduce job jar file: " + mapReduceJarFile, e);
        }
        
        Manifest manifest = jarFile.getManifest();
        if (manifest != null) {
            mainClassName = manifest.getMainAttributes().getValue("Main-Class");
        }
        jarFile.close();
        
        if (mainClassName == null) {
            logger.debug("Main class is not set in jar file, checking arguments");
            if (jobArgs.length < 1) {
                throw new Exception("missing argument: main class");
            }
            mainClassName = jobArgs[currentArg++];
        }
        mainClassName = mainClassName.replaceAll("/", ".");
        logger.debug("Main class identified: {}", mainClassName);
        
        final File workDir = new File(tmpDir, "hadoop-unjar");
        if (!workDir.mkdirs()) {
            throw new IOException("create tmp dir 'hadoop-unjar' failed: " + workDir);
        }
        
        Runtime.getRuntime().addShutdownHook(new Thread() {
            public void run() {
                try {
                    FileUtil.fullyDelete(workDir);
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        });
        
        unJar(mapReduceJarFile, workDir, MATCH_ANY);
        
        ArrayList<URL> classPath = new ArrayList<URL>();
        classPath.add(new File(workDir + "/").toURI().toURL());
        classPath.add(mapReduceJarFile.toURI().toURL());
        classPath.add(new File(workDir, "classes/").toURI().toURL());
        File[] libs = new File(workDir, "lib").listFiles();
        if (libs != null) {
            for (int i = 0; i < libs.length; i++) {
                classPath.add(libs[i].toURI().toURL());
            }
        }
        
        for (File configFile : confFiles) {
            classPath.add(configFile.toURI().toURL());
        }
        
        logger.debug("classpath set: {}", classPath);
        
        ClassLoader loader = new URLClassLoader(classPath.toArray(new URL[0]));        
        Thread.currentThread().setContextClassLoader(loader);
        
        Class<?> mainClass = Class.forName(mainClassName, true, loader);
        Method main = mainClass.getMethod("main", new Class[] { Array.newInstance(String.class, 0).getClass() });
        String[] newArgs = Arrays.asList(jobArgs).subList(currentArg, jobArgs.length).toArray(new String[0]);
        
        main.invoke(null, new Object[] { newArgs });
    }
    
    private void unJar(File jarFile, File toDir, Pattern unpackRegex) throws IOException {
        JarFile jar = new JarFile(jarFile);
        try {
            Enumeration<JarEntry> entries = jar.entries();
            while (entries.hasMoreElements()) {
                JarEntry entry = (JarEntry) entries.nextElement();
                
                if (!entry.isDirectory() && unpackRegex.matcher(entry.getName()).matches()) {
                    InputStream in = jar.getInputStream(entry);
                    
                    try {
                        File file = new File(toDir, entry.getName());
                        ensureDirectory(file.getParentFile());
                        OutputStream out = new FileOutputStream(file);
                        try {
                            IOUtils.copyBytes(in, out, 8192);
                        } finally {
                            out.close();
                        }
                    } finally {
                        in.close();
                    }
                }
            }
        } finally {
            jar.close();
        }
    }
    
    private void ensureDirectory(File dir) throws IOException {
        if (!dir.mkdirs() && !dir.isDirectory()) {
            throw new IOException("Mkdirs failed to create " + dir.toString());
        }
    }
}
