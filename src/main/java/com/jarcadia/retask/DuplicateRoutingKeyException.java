package com.jarcadia.retask;

public class DuplicateRoutingKeyException extends RuntimeException {

    public DuplicateRoutingKeyException(String message)
    {
        super(message);
    }

    public DuplicateRoutingKeyException(Throwable cause)
    {
        super(cause);
    }

    public DuplicateRoutingKeyException(String message, Throwable cause)
    {
        super(message, cause);
    }
}
