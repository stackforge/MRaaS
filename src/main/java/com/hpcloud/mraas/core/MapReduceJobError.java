package com.hpcloud.mraas.core;

public class MapReduceJobError {
    private String errorMessage;
    private Throwable exception;
    
    public MapReduceJobError(String errorMessage, Throwable exception) {
        this.errorMessage = errorMessage;
        this.exception = exception;
    }
    
    public String errorMessage() {
        return errorMessage;
    }
    
    public Throwable exception() {
        return exception;
    }
}