package org.apache.nifi.processors.pravega;

public class ProcessorNotReadyException extends Exception {
    public ProcessorNotReadyException(String s) {
        super(s);
    }
}
