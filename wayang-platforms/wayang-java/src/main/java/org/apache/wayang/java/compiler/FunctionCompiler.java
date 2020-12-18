package org.apache.incubator.wayang.java.compiler;

import org.apache.incubator.wayang.core.api.Configuration;
import org.apache.incubator.wayang.core.function.FlatMapDescriptor;
import org.apache.incubator.wayang.core.function.MapPartitionsDescriptor;
import org.apache.incubator.wayang.core.function.PredicateDescriptor;
import org.apache.incubator.wayang.core.function.ReduceDescriptor;
import org.apache.incubator.wayang.core.function.TransformationDescriptor;

import java.util.function.BinaryOperator;
import java.util.function.Function;
import java.util.function.Predicate;

/**
 * A compiler translates Wayang functions into executable Java functions.
 */
public class FunctionCompiler {

    private final Configuration configuration;

    public FunctionCompiler(Configuration configuration) {
        this.configuration = configuration;
    }

    /**
     * Compile a transformation.
     *
     * @param descriptor describes the transformation
     * @param <I>        input type of the transformation
     * @param <O>        output type of the transformation
     * @return a compiled function
     */
    public <I, O> Function<I, O> compile(TransformationDescriptor<I, O> descriptor) {
        // This is a dummy method but shows the intention of having something compilable in the descriptors.
        return descriptor.getJavaImplementation();
    }

    /**
     * Compile a partition transformation.
     *
     * @param descriptor describes the transformation
     * @param <I>        input type of the transformation
     * @param <O>        output type of the transformation
     * @return a compiled function
     */
    public <I, O> Function<Iterable<I>, Iterable<O>> compile(MapPartitionsDescriptor<I, O> descriptor) {
        // This is a dummy method but shows the intention of having something compilable in the descriptors.
        return descriptor.getJavaImplementation();
    }

    /**
     * Compile a transformation.
     *
     * @param descriptor describes the transformation
     * @param <I>        input type of the transformation
     * @param <O>        output type of the transformation
     * @return a compiled function
     */
    public <I, O> Function<I, Iterable<O>> compile(FlatMapDescriptor<I, O> descriptor) {
        return descriptor.getJavaImplementation();
    }

    /**
     * Compile a reduction.
     *
     * @param descriptor describes the transformation
     * @param <Type>     input/output type of the transformation
     * @return a compiled function
     */
    public <Type> BinaryOperator<Type> compile(ReduceDescriptor<Type> descriptor) {
        // This is a dummy method but shows the intention of having something compilable in the descriptors.
        return descriptor.getJavaImplementation();
    }

    public <Type> Predicate<Type> compile(PredicateDescriptor<Type> predicateDescriptor) {
        return predicateDescriptor.getJavaImplementation();
    }

    public Configuration getConfiguration() {
        return this.configuration;
    }
}