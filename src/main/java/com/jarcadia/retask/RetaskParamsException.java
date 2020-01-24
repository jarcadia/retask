package com.jarcadia.retask;

public class RetaskParamsException extends Exception {

    public RetaskParamsException(String message)
    {
        super(message);
    }

    public RetaskParamsException(Throwable cause)
    {
        super(cause);
    }

    public RetaskParamsException(String message, Throwable cause)
    {
        super(message, cause);
    }
}
