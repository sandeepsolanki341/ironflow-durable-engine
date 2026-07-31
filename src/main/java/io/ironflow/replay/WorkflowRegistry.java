package io.ironflow.replay;

import io.ironflow.api.UnknownWorkflowTypeException;
import io.ironflow.sdk.Workflow;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Resolves workflow type names to implementations.
 *
 * <p>Built from all {@link Workflow} beans at startup, with duplicate type names rejected
 * eagerly. A duplicate registration is otherwise resolved arbitrarily by bean ordering,
 * producing a system that silently runs the wrong code - and does so consistently enough in
 * one environment to pass tests, then differently in another.</p>
 */
@Component
public class WorkflowRegistry {

    private final Map<String, Workflow<?, ?>> byType;

    public WorkflowRegistry(List<Workflow<?, ?>> workflows) {
        List<String> duplicates = workflows.stream()
                .collect(Collectors.groupingBy(Workflow::type, Collectors.counting()))
                .entrySet().stream()
                .filter(e -> e.getValue() > 1)
                .map(Map.Entry::getKey)
                .toList();
        if (!duplicates.isEmpty()) {
            throw new IllegalStateException("Duplicate workflow types registered: " + duplicates);
        }
        this.byType = workflows.stream()
                .collect(Collectors.toUnmodifiableMap(Workflow::type, Function.identity()));
    }

    public boolean isRegistered(String type) {
        return byType.containsKey(type);
    }

    public Set<String> registeredTypes() {
        return byType.keySet();
    }

    @SuppressWarnings("unchecked")
    public Workflow<Object, Object> resolve(String type) {
        Workflow<?, ?> wf = byType.get(type);
        if (wf == null) {
            throw new UnknownWorkflowTypeException(type, registeredTypes());
        }
        return (Workflow<Object, Object>) wf;
    }
}
