package com.mynameis.log;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by reaneyp on 9/15/17.
 */

public class LogUtils {

    private static final ClassFinder classFinder = new ClassFinder();

    /**
     * Returns the org.slf.Logger associated with the class that called this method.
     *
     * This is a less verbose alternative to calling
     * LoggerFactory.getLogger(className) directly, which is prone to copy-and-paste
     * errors.
     *
     * This method is intended to be called once at class initialization and the
     * result stored in a static final variable. Calling this class repeatedly on
     * demand when logging is not recommended,
     * 
     * as the performance of this method has not been tested.
     *
     * 
     * 
     * @return the org.slf.Logger associated with the class that called this method.
     * 
     */

    public static Logger getLogger() {
        @SuppressWarnings("rawtypes")
        final Class[] context = classFinder.getClassArray();
        Class<?> callerClass = LogUtils.class;
        for (int i = 0; i < context.length; i++) {
            if (context[i] == LogUtils.class && context.length > i + 1) {
                callerClass = context[i + 1];
                break;
            }
        }
        return LoggerFactory.getLogger(callerClass);
    }

    private static class ClassFinder extends SecurityManager {
        @SuppressWarnings("rawtypes")
        private Class[] getClassArray() {
            return getClassContext();
        }
    }
}