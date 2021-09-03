package dev.jarcadia;

import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.ObjectMapper;
import dev.jarcadia.iface.CustomParamProvider;
import io.micronaut.core.type.Argument;
import io.micronaut.inject.ExecutableMethod;

import java.util.ArrayList;
import java.util.List;

class MicronautParamProducerFactory {

    private final ObjectMapper objectMapper;

    protected MicronautParamProducerFactory(ObjectMapper objectMapper) {
        this.objectMapper = objectMapper;
    }

    protected MicronautTaskHandlerParamProducer forTask(ExecutableMethod method, List<CustomParamProvider> cpps) {
        return create(method, StaticParamType.TASK, cpps);
    }

    protected MicronautTaskHandlerParamProducer forDmlHandler(ExecutableMethod method, List<CustomParamProvider> cpps) {
        return create(method, StaticParamType.DML_EVENT, cpps);
    }

    private MicronautTaskHandlerParamProducer create(ExecutableMethod method, StaticParamType staticParamType,
            List<CustomParamProvider> cpps) {

        try {
            Argument[] args = method.getArguments();
            List<StaticParam> statics = new ArrayList<>();
            List<DynamicParam> dynamics = new ArrayList<>();

            for (int i=0; i<args.length; i++) {
                Argument arg = args[i];

                // Check for statics
                for (int j=0; j<staticParamType.getDefs().length; j++) {
                    StaticParamType.Def spd = staticParamType.getDefs()[j];
                    if (spd.name().equals(arg.getName())) {
                        if (arg.getType().equals(spd.type())) {
                            statics.add(new StaticParam(j, i, spd.name(), spd.type()));
                            continue;
                        } else {
                            throw new ParamException(String.format("Expected static param %s to be of type % but is %",
                                    spd.name(), spd.type().getSimpleName(), arg.getType().getSimpleName()));
                        }
                    }
                }

                // If not static, add as a dynamic field
                String name = arg.getName(); // TODO handle @Named
                JavaType javaType = objectMapper.constructType(new MicronautParameterizedType(arg.asParameterizedType()));
                dynamics.add(new DynamicParam(i, name, arg.getType(), javaType));
            }

            return new MicronautTaskHandlerParamProducer(args.length, staticParamType, statics.toArray(new StaticParam[0]),
                    dynamics.toArray(new DynamicParam[0]), cpps == null ? null : cpps.toArray(new CustomParamProvider[0]));
        } catch (Throwable t) {
            throw new ParamException(String.format("Unable to create TaskHandlerParamProducer for %s.%s",
                    method.getDeclaringType().getSimpleName(), method.getName()), t);
        }
    }


}
