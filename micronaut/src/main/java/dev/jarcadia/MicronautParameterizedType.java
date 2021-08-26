package dev.jarcadia;

import io.micronaut.core.type.DefaultArgument;

import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;

/**
 * Jackson doesn't like the ParameterizedType produced by Micronaut in some cases, like Optional<BigInteger>.
 * Micronaut will return 'BigInteger T' instead of BigInteger.class. This class intercepts that scenario and returns
 * the underlying class so Jackson can build a proper JavaType.
 */
public class MicronautParameterizedType implements ParameterizedType {

    private final ParameterizedType source;

    public MicronautParameterizedType(ParameterizedType source) {
        this.source = source;
    }

    @Override
    public Type[] getActualTypeArguments() {
        Type[] typeArgs = source.getActualTypeArguments();
        if (typeArgs == null) {
            return null;
        }
        Type[] result = new Type[typeArgs.length];
        for (int i=0; i<typeArgs.length; i++) {
            if (typeArgs[i] instanceof DefaultArgument) {
                DefaultArgument defArg = (DefaultArgument) typeArgs[i];
                result[i] = defArg.getType();
            } else {
                result[i] = typeArgs[i];
            }
        }
        return result;
    }

    @Override
    public Type getRawType() {
        return source.getRawType();
    }

    @Override
    public Type getOwnerType() {
        return source.getOwnerType();
    }
}
