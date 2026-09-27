package com.kyaacdc.emailnotifymicroservice.exception;

public class RetryableException extends RuntimeException{

    public RetryableException(String message) {
        super(message);
    }

    public RetryableException(Throwable cause) {
        super(cause);
    }
}
