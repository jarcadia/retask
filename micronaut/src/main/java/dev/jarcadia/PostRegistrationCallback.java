package dev.jarcadia;

@FunctionalInterface
public interface PostRegistrationCallback {
    void apply(RetaskMicronautRegistations retaskMicronautRegistations);
}
