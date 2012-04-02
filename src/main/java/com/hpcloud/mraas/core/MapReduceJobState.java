package com.hpcloud.mraas.core;

public enum MapReduceJobState {
    NOT_DEFINED(0),
    INITIALIZED(1),
    RUNNING(2),
    RETURNED(3),
    COMPLETED(4),
    CLUSTER_HAS_JOBS(5),
    EXCEPTION(6),
    ERROR(7);
    
    private final int state;
    
    MapReduceJobState(int state) {
        this.state = state;
    }
    
    public int state() {
        return state;
    }
    
    public static MapReduceJobState state(int state) {
        if (state < 0 || state >= MapReduceJobState.values().length) {
            return null;
        }
        
        return MapReduceJobState.values()[state];
    }
}