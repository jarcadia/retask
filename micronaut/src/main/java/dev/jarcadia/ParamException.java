package dev.jarcadia;

class ParamException extends RuntimeException {

    protected ParamException(String message) {
        super(message);
    }

    protected ParamException(String message, Throwable cause) {
        super(message, cause);
    }

    protected ParamException(Throwable cause) {
        super(cause);
    }
}
