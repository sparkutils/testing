package com.sparkutils.testing;

import com.google.common.collect.ImmutableSet;
import com.google.common.reflect.ClassPath;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;

/**
 * Simple class to run all tests found in classloader in batches or start from a specific batch
 */
public class TestRunner {

    private final ClassLoader classLoader;
    public TestRunner(ClassLoader classLoader){
        this.classLoader = classLoader;
    }

    public void testViaClassLoader(int batchStartingNumber) throws IOException {
        testViaClassLoader(new String[]{}, batchStartingNumber);
    }
    public void testViaClassLoader(String[] args) throws IOException {
        testViaClassLoader(args, 0);
    }

    public void testViaClassLoader(String[] args, int batchStartingNumber) throws IOException {
        ArrayList<String> oargs = new ArrayList<String>();
        oargs.add("-oWDFT");
        for (int i = 0; i < args.length; i++) {
            oargs.add(args[i]);
        }

        ArrayList<String> classargs = new ArrayList<>();
        ClassPath classPath = ClassPath.from(classLoader);
        ImmutableSet<ClassPath.ClassInfo> framelessInfo = classPath.getTopLevelClassesRecursive("frameless");
        for (ClassPath.ClassInfo i: framelessInfo) {
            try {
                Class<?> clazz = i.load();
                org.scalatest.Suite suite = (org.scalatest.Suite) clazz.newInstance();
                // it's a suite
                classargs.add("-s");
                classargs.add(clazz.getName());
            } catch (Throwable t) {
                // ignore
            }
        }

        int numberOfBatches = classargs.size() / 2 / 10;
        int argsPerBatch = 20;

        Iterator<String> classargItr = classargs.iterator();

        for (int j = 0; j < (batchStartingNumber * argsPerBatch) && classargItr.hasNext(); j++) {
            classargItr.next();
        }

        for (int i = batchStartingNumber; i < numberOfBatches; i++) {
            System.out.println("testless - starting batch "+i);
            String[] joined = new String[oargs.size() + argsPerBatch];
            oargs.<String>toArray(joined);
            for (int j = 0; j < argsPerBatch && classargItr.hasNext(); j++) {
                joined[oargs.size() + j] = classargItr.next();
            }
            org.scalatest.tools.Runner.run(joined);
            System.out.println("testless - gc'ing after finishing batch "+i);
            System.gc();
            System.gc();
        }
        System.out.println("all testless batches completed");
    }

    /**
     * Run against any test found in the current classloader
     * @param args
     */
    public static void scalaTestRunner(String[] args){
        org.scalatest.tools.Runner.run(args);
    }

    public static void test(int batchStartingNumber) throws IOException {
        test(new String[]{}, batchStartingNumber);
    }
    public static void test(String[] args) throws IOException {
        test(args, 0);
    }

    public static void test(String[] args, int batchStartingNumber) throws IOException {
        new TestRunner(TestRunner.class.getClassLoader()).testViaClassLoader(args, batchStartingNumber);
    }

    public static void runTestName(String testName) throws IOException {
        ArrayList<String> oargs = new ArrayList<String>();
        oargs.add("-oWDFT");

        oargs.add("-s");
        oargs.add(testName);

        String[] joined = new String[oargs.size()];
        oargs.<String>toArray(joined);
        org.scalatest.tools.Runner.run(joined);
        System.out.println("testless finished properly");
    }

    public static void test() throws IOException {
        test(new String[]{});
    }

}
