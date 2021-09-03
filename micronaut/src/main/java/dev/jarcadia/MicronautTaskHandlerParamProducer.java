package dev.jarcadia;

import dev.jarcadia.iface.CustomParamProvider;

import java.util.HashMap;
import java.util.Map;

class MicronautTaskHandlerParamProducer {

    private final int length;
    private final StaticParamType staticParamType;
    private final StaticParam[] staticParams;
    private final DynamicParam[] dynamicParams;
    private final CustomParamProvider[] customParamProviders;

    protected MicronautTaskHandlerParamProducer(int length, StaticParamType staticParamType,
            StaticParam[] staticParams, DynamicParam[] dynamicParams,
            CustomParamProvider[] customParamProviders) {
        this.length = length;
        this.staticParamType = staticParamType;
        this.staticParams = staticParams;
        this.dynamicParams = dynamicParams;
        this.customParamProviders = customParamProviders;
    }

    protected Object[] produceParams(Object[] statics, Fields fields) {
        Object[] result = new Object[length];

        for (StaticParam sp : staticParams) {
            result[sp.paramIndex()] = statics[sp.staticIndex()];
        }

        for (DynamicParam fp : dynamicParams) {
            result[fp.index()] = fields.getFieldAs(fp.name(), fp.javaType());
        }

        if (customParamProviders != null && customParamProviders.length > 0) {

            // Map that name->value to all available statics (including those NOT present in params)
            Map<String, Object> staticsMap = new HashMap<>();
            for (int i=0; i<staticParamType.getDefs().length; i++) {
                staticsMap.put(staticParamType.getDefs()[i].name(), statics[i]);
            }

            // Create and populate settable array that will allow the CustomParamProvider to add/modify values
            SettableParam[] settables = new SettableParam[length];
            for (int i=0; i<staticParams.length; i++) {
                StaticParam sp = staticParams[i];
                settables[sp.paramIndex()] = new SettableParam(sp.name(), sp.type(), result[sp.paramIndex()]);
            }
            for (int i=0; i<dynamicParams.length; i++) {
                DynamicParam dp = dynamicParams[i];
                settables[i] = new SettableParam(dp.name(), dp.type(), result[dp.index()]);
            }

            // Let each CustomParamProvider customize params
            for (CustomParamProvider cpp : customParamProviders) {
                cpp.apply(staticsMap, fields, settables);
            }

            // Apply customizations to result
            for (int i=0; i<length; i++) {
                result[i] = settables[i].get();
            }
        }

        return result;
    }
}
