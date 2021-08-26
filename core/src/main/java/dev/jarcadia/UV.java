package dev.jarcadia;

public class UV<T> {

    private final T pre;
    private final T post;

    public UV(T pre, T post) {
        this.pre = pre;
        this.post = post;
    }

    public T pre() {
        return pre;
    }

    public T post() {
        return post;
    }
}
