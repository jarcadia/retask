package dev.jarcadia.iface;

import dev.jarcadia.Fields;
import dev.jarcadia.SettableParam;

import java.util.Map;

@FunctionalInterface
public interface CustomParamProvider {
    void apply(Map<String, Object> statics, Fields fields, SettableParam[] settables);
}
