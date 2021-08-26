package dev.jarcadia.exception;

public class PubSubException extends RuntimeException {

    public PubSubException(String message)
    {
        super(message);
    }

    public PubSubException(Throwable cause)
    {
        super(cause);
    }

    public PubSubException(String message, Throwable cause)
    {
        super(message, cause);
    }
}
