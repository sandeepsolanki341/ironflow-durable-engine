package io.ironflow.worker;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.stereotype.Component;

import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Resolves activity type names to invocable methods.
 *
 * <p>Scans beans for {@link Activity}-annotated methods at startup, rejecting duplicate
 * type names eagerly for the same reason {@code WorkflowRegistry} does: a duplicate is
 * otherwise resolved arbitrarily by bean ordering, producing a system that silently runs
 * the wrong code.</p>
 */
@Component
public class ActivityRegistry {

    private final Map<String, Registration> byType = new HashMap<>();
    private final ObjectMapper mapper;

    public ActivityRegistry(List<Object> beans, ObjectMapper mapper) {
        this.mapper = mapper;
        for (Object bean : beans) {
            for (Method m : bean.getClass().getMethods()) {
                Activity annotation = m.getAnnotation(Activity.class);
                if (annotation == null) {
                    continue;
                }
                Registration prior = byType.put(annotation.value(),
                        new Registration(bean, m));
                if (prior != null) {
                    throw new IllegalStateException(
                            "Duplicate activity type '%s' on %s and %s".formatted(
                                    annotation.value(),
                                    prior.method(), m));
                }
            }
        }
    }

    public boolean isRegistered(String type) {
        return byType.containsKey(type);
    }

    public Set<String> registeredTypes() {
        return byType.keySet();
    }

    /**
     * Invokes an activity, unwrapping reflection wrappers.
     *
     * <p>Unwrapping matters: {@code InvocationTargetException} would otherwise mask the
     * user's actual exception type, and {@code ActivityOptions.nonRetryableErrors} would
     * never match anything.</p>
     *
     * @throws Throwable the activity's own exception, unwrapped
     */
    public JsonNode invoke(String activityType, JsonNode input, ActivityContext ctx)
            throws Throwable {
        Registration reg = byType.get(activityType);
        if (reg == null) {
            throw new UnknownActivityTypeException(activityType, registeredTypes());
        }
        try {
            Class<?>[] params = reg.method().getParameterTypes();
            Object typedInput = params.length > 0
                    ? mapper.treeToValue(input, params[0]) : null;

            Object result = switch (params.length) {
                case 0 -> reg.method().invoke(reg.bean());
                case 1 -> reg.method().invoke(reg.bean(), typedInput);
                default -> reg.method().invoke(reg.bean(), typedInput, ctx);
            };
            return result == null ? mapper.nullNode() : mapper.valueToTree(result);

        } catch (java.lang.reflect.InvocationTargetException e) {
            throw e.getCause() == null ? e : e.getCause();
        }
    }

    private record Registration(Object bean, Method method) { }
}
