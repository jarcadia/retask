package dev.jarcadia;

import dev.jarcadia.annontation.OnDelete;
import dev.jarcadia.annontation.OnInsert;
import dev.jarcadia.annontation.OnStart;
import dev.jarcadia.annontation.OnUpdate;
import dev.jarcadia.annontation.TaskHandler;
import dev.jarcadia.iface.CustomParamProvider;
import io.micronaut.context.BeanContext;
import io.micronaut.core.annotation.AnnotationValue;
import io.micronaut.inject.BeanDefinition;
import io.micronaut.inject.ExecutableMethod;
import io.micronaut.inject.qualifiers.Qualifiers;

import java.lang.annotation.Annotation;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

public class RetaskMicronaut {

    public static RetaskMicronautConfig configure(Retask retask, BeanContext beanContext) {
        return new RetaskMicronautConfig(retask, beanContext);
    }

    protected static RetaskMicronautRegistations initialize(RetaskMicronautConfig config) {

        Retask retask = config.getJarcadia();
        BeanContext context = config.getBeanContext();

        MicronautParamProducerFactory micronautParamProducerFactory =
                new MicronautParamProducerFactory(retask.getObjectMapper());

        Collection<BeanDefinition<?>> definitions = context
                .getBeanDefinitions(Qualifiers.byStereotype(TaskHandler.class));

        List<RegisteredTaskHandler<?>> registeredTaskHandlers = new ArrayList<>();

        Map<Class<? extends Annotation>, List<CustomParamProvider>> cppMap =
                config.getRegisteredCustomParamProviders().stream()
                        .collect(Collectors.groupingBy(RegisteredCustomParamProvider::type,
                                Collectors.mapping(RegisteredCustomParamProvider::provider,
                                        Collectors.toList())));


        for(BeanDefinition definition : definitions) {

            Collection<ExecutableMethod> methods = definition.getExecutableMethods();
            for (ExecutableMethod method : methods) {

                Optional<AnnotationValue<OnStart>> onStart = method.findAnnotation(OnStart.class);
                if (onStart.isPresent()) {
                    retask.registerStartHandler(new MicronautStartHandler<>(context, definition, method));
                }

                for (RegisteredTaskHandlerAnnotation<?> registeredTaskHandlerAnnotation :
                        config.getRegisteredTaskHandlerAnnotations()) {
                    RegisteredTaskHandler<?> handler = registeredTaskHandlerAnnotation.check(definition, method);
                    if (handler != null) {
                        registeredTaskHandlers.add(handler);
                        MicronautTaskHandlerParamProducer paramProducer = micronautParamProducerFactory
                                .forTask(method, cppMap.get(handler.annotationType()));
                        retask.registerTaskHandler(handler.route(),
                                new MicronautTaskHandler<>(context, definition, method, paramProducer));
                    }
                }

                Optional<AnnotationValue<OnInsert>> onInsert = method.findAnnotation(OnInsert.class);
                if (onInsert.isPresent()) {
                    String table = onInsert.get().stringValue("table").get();
                    MicronautTaskHandlerParamProducer paramProducer = micronautParamProducerFactory
                            .forDmlHandler(method, cppMap.get(OnInsert.class));
                    retask.registerInsertHandler(table,
                            new MicronautDmlEventHandler<>(context, definition, method, paramProducer));
                }

                Optional<AnnotationValue<OnUpdate>> onUpdate = method.findAnnotation(OnUpdate.class);
                if (onUpdate.isPresent()) {
                    String table = onUpdate.get().stringValue("table").get();
                    MicronautTaskHandlerParamProducer paramProducer = micronautParamProducerFactory
                            .forDmlHandler(method, cppMap.get(OnUpdate.class));
                    retask.registerUpdateHandler(table,
                            new MicronautDmlEventHandler<>(context, definition, method, paramProducer));
                }

                Optional<AnnotationValue<OnDelete>> onDelete = method.findAnnotation(OnDelete.class);
                if (onDelete.isPresent()) {
                    String table = onDelete.get().stringValue("table").get();
                    MicronautTaskHandlerParamProducer paramProducer = micronautParamProducerFactory
                            .forDmlHandler(method, cppMap.get(OnDelete.class));
                    retask.registerDeleteHandler(table,
                            new MicronautDmlEventHandler<>(context, definition, method, paramProducer));
                }
            }
        }

        RetaskMicronautRegistations retaskMicronautRegistations = new RetaskMicronautRegistations(registeredTaskHandlers);

        return retaskMicronautRegistations;
    }
}
