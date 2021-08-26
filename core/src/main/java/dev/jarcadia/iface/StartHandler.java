package dev.jarcadia.iface;

@FunctionalInterface
public interface StartHandler {

    Object run() throws Throwable;

}