package dev.jarcadia;

import dev.jarcadia.iface.CustomParamProvider;

import java.lang.annotation.Annotation;

record RegisteredCustomParamProvider<T extends Annotation>(Class<T> type,
                                                                 CustomParamProvider provider) { }
