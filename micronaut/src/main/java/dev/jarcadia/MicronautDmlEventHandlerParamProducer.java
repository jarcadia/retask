//package dev.jarcadia;
//
//import dev.jarcadia.iface.CustomParamProvider;
//
//class MicronautDmlEventHandlerParamProducer {
//
//    private final int length;
//    private final int tableIdIndex;
//    private final DynamicParam[] dynamicParams;
//    private final CustomParamProvider[] customParamProviders;
//
//    protected MicronautDmlEventHandlerParamProducer(int length, int tableIdIndex, DynamicParam[] dynamicParams,
//            CustomParamProvider[] customParamProviders) {
//        this.length = length;
//        this.tableIdIndex = tableIdIndex;
//        this.dynamicParams = dynamicParams;
//        this.customParamProviders = customParamProviders;
//    }
//
//    protected Object[] produceParams(String table, Fields fields) {
//        Object[] result = new Object[length];
//        if (tableIdIndex != -1) result[tableIdIndex] = table;
//
//        for (DynamicParam fp : dynamicParams) {
//            Object paramValue = fields.getFieldAs(fp.name(), fp.type());
//            if (paramValue == null && customParamProviders != null) {
//                for (CustomParamProvider cpp : customParamProviders) {
//                    paramValue = cpp.get(fp.name(), fp.type());
//                    if (paramValue != null) break;
//                }
//            }
//            result[fp.index()] = paramValue;
//        }
//        return result;
//    }
//}
