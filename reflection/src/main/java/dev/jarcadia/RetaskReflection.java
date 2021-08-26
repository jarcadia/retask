package dev.jarcadia;

import dev.jarcadia.annontation.OnDelete;
import dev.jarcadia.annontation.OnInsert;
import dev.jarcadia.annontation.OnStart;
import dev.jarcadia.annontation.OnTask;
import dev.jarcadia.annontation.OnUpdate;

import java.util.HashMap;
import java.util.Map;

public class RetaskReflection {

    public static void attach(Jarcadia jarcadia, ScanConfig config) {
        AnnotatedHandlerScanner annotatedHandlerScanner = new AnnotatedHandlerScanner();
        Map<Class<?>, Object> providedInstances = new HashMap<>();
        annotatedHandlerScanner.addInstanceProvider(type -> providedInstances.get(type));

        for (InstanceProvider instanceProvider : config.getInstanceProviders()) {
            annotatedHandlerScanner.addInstanceProvider(instanceProvider);
        }
        for (String packageName : config.getScanPackages()) {
            annotatedHandlerScanner.addPackage(packageName);
        }
//        annotatedHandlerScanner.registerMethodAnnotation(OnStart.class,
//                (register, annotation, method, instance) -> register.asStartHandler());
//        annotatedHandlerScanner.registerMethodAnnotation(OnTask.class,
//                (register, annotation, method, instance) -> register.asTaskHandler(annotation.route()));
//        annotatedHandlerScanner.registerMethodAnnotation(OnInsert.class,
//                (register, annotation, method, instance) -> register.asInsertHandler(annotation.type()));
//        annotatedHandlerScanner.registerMethodAnnotation(OnUpdate.class,
//                (register, annotation, method, instance) ->
//                        register.asUpdateHandler(annotation.type(), annotation.fields()));
//        annotatedHandlerScanner.registerMethodAnnotation(OnDelete.class,
//                (register, annotation, method, instance) -> register.asDeleteHandler(annotation.type()));

        annotatedHandlerScanner.scan(jarcadia);
    }
}
