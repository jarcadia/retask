package dev.jarcadia;

@FunctionalInterface
public interface PostRegistrationCallback {
    void apply(RetaskRegistations retaskRegistations);
}
