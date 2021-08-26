package dev.jarcadia.exception;

public class CalledTaskException extends Exception {

    public CalledTaskException(String message)
    {
        super(message);
    }

    public CalledTaskException(Throwable cause)
    {
        super(cause);
    }

    public CalledTaskException(String message, Throwable cause)
    {
        super(message, cause);
    }
}
