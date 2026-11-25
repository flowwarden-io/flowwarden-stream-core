/*
 * Copyright 2026 FlowWarden
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.flowwarden.stream.internal.discovery;

import org.springframework.core.BridgeMethodResolver;
import org.springframework.core.ResolvableType;
import org.springframework.core.annotation.AnnotationUtils;
import org.springframework.util.ClassUtils;
import org.springframework.util.ReflectionUtils;

import java.lang.annotation.Annotation;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Looks up the annotated methods of a {@code @ChangeStream} class through its whole type
 * hierarchy: the class itself, its superclasses and the interfaces it implements (default
 * methods included).
 *
 * <p>Each method is reduced to its most specific version on the concrete class, so a method
 * declared in a superclass and overridden in a subclass counts once, as the override. The
 * annotation is read on the method that declares it and kept through that reduction: an
 * override inherits it, and so does an implementation the class inherits from a superclass
 * that does not itself implement the annotated interface. Two methods that do not override
 * each other stay distinct, wherever they are declared, and are subject to the usual
 * cardinality rules.</p>
 */
final class AnnotatedMethods {

    private AnnotatedMethods() {
    }

    /**
     * Returns the methods of {@code targetClass} carrying {@code annotationType}, inherited
     * ones included, in a stable order (most specific declaring class first, then by name).
     */
    static List<Method> find(Class<?> targetClass, Class<? extends Annotation> annotationType) {
        return new ArrayList<>(findWithAnnotation(targetClass, annotationType).keySet());
    }

    /**
     * Same as {@link #find}, with the annotation each method was selected for. The annotation
     * must be taken from here rather than looked up again on the returned method: when it is
     * declared on an interface and the implementation comes from a superclass, the returned
     * method cannot see it. When both the implementation and a declaration it implements are
     * annotated, the one closest to the implementation wins.
     */
    static <A extends Annotation> Map<Method, A> findWithAnnotation(Class<?> targetClass,
                                                                     Class<A> annotationType) {
        Class<?> userClass = ClassUtils.getUserClass(targetClass);
        Set<Class<?>> types = new LinkedHashSet<>();
        types.add(userClass);
        types.addAll(ClassUtils.getAllInterfacesForClassAsSet(userClass));

        Map<Method, A> found = new HashMap<>();
        for (Class<?> type : types) {
            ReflectionUtils.doWithMethods(type, declared -> {
                A declaredAnnotation = AnnotationUtils.findAnnotation(declared, annotationType);
                if (declaredAnnotation == null) {
                    return;
                }
                Method specific = BridgeMethodResolver.findBridgedMethod(
                        ClassUtils.getMostSpecificMethod(declared, userClass));
                A ownAnnotation = AnnotationUtils.findAnnotation(specific, annotationType);
                if (ownAnnotation != null) {
                    found.put(specific, ownAnnotation);
                } else {
                    found.putIfAbsent(specific, declaredAnnotation);
                }
            }, ReflectionUtils.USER_DECLARED_METHODS);
        }

        List<Method> ordered = new ArrayList<>(found.keySet());
        ordered.sort(Comparator
                .comparingInt((Method method) -> depth(userClass, method.getDeclaringClass()))
                .thenComparing(Method::getName)
                .thenComparing(Method::toGenericString));
        Map<Method, A> result = new LinkedHashMap<>();
        for (Method method : ordered) {
            result.put(method, found.get(method));
        }
        return result;
    }

    /**
     * Resolves the type of a method parameter against the concrete class, so that a parameter
     * declared as a type variable in a generic superclass ({@code void onInsert(T document)})
     * is seen with the type the subclass binds it to. Falls back to the erased type when the
     * variable is not bound.
     */
    static Class<?> parameterType(Method method, int index, Class<?> targetClass) {
        Class<?> erased = method.getParameterTypes()[index];
        return ResolvableType.forMethodParameter(method, index, targetClass).resolve(erased);
    }

    /** Lists the methods as {@code DeclaringClass#method}, for cardinality error messages. */
    static String describe(List<Method> methods) {
        return methods.stream()
                .map(method -> method.getDeclaringClass().getName() + "#" + method.getName())
                .collect(Collectors.joining(", "));
    }

    /** Number of superclass hops from {@code targetClass} to {@code declaringClass}; interfaces come last. */
    private static int depth(Class<?> targetClass, Class<?> declaringClass) {
        int depth = 0;
        for (Class<?> current = targetClass; current != null; current = current.getSuperclass()) {
            if (current == declaringClass) {
                return depth;
            }
            depth++;
        }
        return Integer.MAX_VALUE;
    }
}
