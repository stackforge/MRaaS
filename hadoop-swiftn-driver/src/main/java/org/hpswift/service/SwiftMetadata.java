package org.hpswift.service;

public class SwiftMetadata {
    private final long length;
    private final long lastModified;

    public SwiftMetadata(long length, long lastModified) {
        this.length = length;
        this.lastModified = lastModified;
    }

    public long getLength() {
        return length;
    }

    public long getLastModified() {
        return lastModified;
    }

    @Override
    public String toString() {
        return "SwiftMetadata[" + length + ", " + lastModified + "]";
    }

}
