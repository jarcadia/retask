package dev.jarcadia.exception;

public class RetaskException extends RuntimeException {

    public RetaskException(String message)
    {
        super(message);
    }

    public RetaskException(Throwable cause)
    {
        super(cause);
    }

    public RetaskException(String message, Throwable cause)
    {
        super(message, cause);
    }
}
