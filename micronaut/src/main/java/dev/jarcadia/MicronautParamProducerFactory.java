package dev.jarcadia;

import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.ObjectMapper;
import dev.jarcadia.annontation.OnTask;
import dev.jarcadia.annontation.OnUpdate;
import io.micronaut.core.annotation.AnnotationValue;
import io.micronaut.core.type.Argument;
import io.micronaut.inject.ExecutableMethod;

import java.util.stream.Collectors;
import java.util.stream.IntStream;

class MicronautParamProducerFactory {

    private final ObjectMapper objectMapper;

    protected MicronautParamProducerFactory(ObjectMapper objectMapper) {
        this.objectMapper = objectMapper;
    }

    protected MicronautTaskHandlerParamProducer forTask(ExecutableMethod method) {

        try {
            Argument[] args = method.getArguments();
            int taskIdIndex = findStaticParam(args, "taskId", int.class);
            int routeIndex = findStaticParam(args, "route", String.class);
            int attemptIndex = findStaticParam(args, "attempt", int.class);
            int permitIndex = findStaticParam(args, "permit", int.class);

            FieldParam[] fieldParams = IntStream.range(0, args.length)
                    .filter(i -> i != taskIdIndex && i != routeIndex && i != attemptIndex && i != permitIndex)
                    .mapToObj(i -> setupDynamicParam(i, args[i]))
                    .collect(Collectors.toList())
                    .toArray(new FieldParam[0]);
            return new MicronautTaskHandlerParamProducer(args.length, taskIdIndex, routeIndex,
                    attemptIndex, permitIndex, fieldParams);
        } catch (Throwable t) {
            throw new ParamException(String.format("Unable to create TaskHandlerParamProducer for %s.%s",
                    method.getDeclaringType().getSimpleName(), method.getName()), t);
        }
    }

    protected MicronautDmlEventHandlerParamProducer forDmlHandler(ExecutableMethod method) {
        try {
            Argument[] args = method.getArguments();
            int tableIndex = findStaticParam(args, "table", String.class);

            FieldParam[] fieldParams = IntStream.range(0, args.length)
                    .filter(i -> i != tableIndex)
                    .mapToObj(i -> setupDynamicParam(i, args[i]))
                    .collect(Collectors.toList())
                    .toArray(new FieldParam[0]);
            return new MicronautDmlEventHandlerParamProducer(args.length, tableIndex, fieldParams);
        } catch (Throwable t) {
            throw new ParamException(String.format("Unable to create TaskHandlerParamProducer for %s.%s",
                    method.getDeclaringType().getSimpleName(), method.getName()), t);
        }
    }

    protected int findStaticParam(Argument[] args, String name, Class<?> clazz) {
        for (int i=0; i<args.length; i++) {
            Argument arg = args[i];
            if (name.equals(arg.getName())) {
                if (!arg.getType().equals(clazz)) {
                    throw new ParamException(String.format("Expected param %s to be of type % but is %",
                            name, clazz.getSimpleName(), arg.getType().getSimpleName()));
                }
                return i;
            }
        }
        return -1;
    }

    protected FieldParam setupDynamicParam(int index, Argument arg) {
        String name = arg.getName(); // TODO handle @Named
        JavaType javaType = objectMapper.constructType(new MicronautParameterizedType(arg.asParameterizedType()));
        return new FieldParam(index, name, javaType);
    }
}
