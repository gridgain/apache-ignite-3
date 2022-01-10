/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.network.serialization.marshal;

import static org.apache.ignite.internal.network.serialization.marshal.ObjectClass.objectClass;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.Externalizable;
import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.Collection;
import java.util.Map;
import org.apache.ignite.internal.network.serialization.BuiltinType;
import org.apache.ignite.internal.network.serialization.ClassDescriptor;
import org.apache.ignite.internal.network.serialization.ClassDescriptorFactory;
import org.apache.ignite.internal.network.serialization.ClassDescriptorFactoryContext;
import org.apache.ignite.internal.network.serialization.IdIndexedDescriptors;
import org.apache.ignite.internal.network.serialization.Null;
import org.apache.ignite.internal.network.serialization.SerializedStreamCommands;
import org.apache.ignite.internal.network.serialization.SpecialMethodInvocationException;
import org.jetbrains.annotations.Nullable;

/**
 * Default implementation of {@link UserObjectMarshaller}.
 */
public class DefaultUserObjectMarshaller implements UserObjectMarshaller {
    private final ClassDescriptorFactoryContext descriptorRegistry;
    private final ClassDescriptorFactory descriptorFactory;

    private final BuiltInNonContainerMarshallers builtInNonContainerMarshallers = new BuiltInNonContainerMarshallers();
    private final BuiltInContainerMarshallers builtInContainerMarshallers = new BuiltInContainerMarshallers(
            (obj, out, ctx) -> marshalToOutput(obj, objectClass(obj), out, ctx)
    );
    private final ArbitraryObjectMarshaller arbitraryObjectMarshaller;
    private final ExternalizableMarshaller externalizableMarshaller;

    /**
     * Constructor.
     *
     * @param descriptorRegistry registry of local descriptors to consult with
     * @param descriptorFactory  descriptor factory to create new descriptors from classes
     */
    public DefaultUserObjectMarshaller(ClassDescriptorFactoryContext descriptorRegistry, ClassDescriptorFactory descriptorFactory) {
        this.descriptorRegistry = descriptorRegistry;
        this.descriptorFactory = descriptorFactory;

        arbitraryObjectMarshaller = new ArbitraryObjectMarshaller(
                descriptorRegistry,
                this::marshalToOutput,
                this::unmarshalFromInput
        );

        externalizableMarshaller = new ExternalizableMarshaller(this::unmarshalFromInput, this::marshalToOutput, arbitraryObjectMarshaller);
    }

    public MarshalledObject marshal(@Nullable Object object) throws MarshalException {
        return marshal(object, objectClass(object));
    }

    /** {@inheritDoc} */
    @Override
    public MarshalledObject marshal(@Nullable Object object, Class<?> declaredClass) throws MarshalException {
        MarshallingContext context = new MarshallingContext();

        var baos = new ByteArrayOutputStream();
        try (var dos = new DataOutputStream(baos)) {
            marshalToOutput(object, declaredClass, dos, context);
        } catch (IOException e) {
            throw new MarshalException("Cannot marshal", e);
        }

        return new MarshalledObject(baos.toByteArray(), context.usedDescriptors());
    }

    private void marshalToOutput(@Nullable Object object, Class<?> declaredClass, DataOutputStream output, MarshallingContext context)
            throws MarshalException, IOException {
        assert declaredClass != null;
        assert object == null
                || declaredClass.isPrimitive()
                || objectIsMemberOfEnumWithAnonymousClassesForMembers(object, declaredClass)
                || declaredClass.isAssignableFrom(object.getClass())
                : "Object " + object + " is expected to have class " + declaredClass + ", but its " + object.getClass();

        throwIfMarshallingNotSupported(object);

        ClassDescriptor originalDescriptor = obtainOriginalDescriptor(object, declaredClass);

        DescribedObject writeReplaced = applyWriteReplaceIfNeeded(object, originalDescriptor);

        if (canParticipateInCycles(writeReplaced.descriptor)) {
            Integer maybeRefId = context.rememberAsSeen(writeReplaced.object);
            if (maybeRefId != null) {
                writeReference(maybeRefId, output);
            } else {
                marshalCycleable(writeReplaced, output, context);
            }
        } else {
            marshalNonCycleable(writeReplaced, output, context);
        }
    }

    /**
     * Returns {@code true} if an instance of the type represented by the descriptor may actively form a cycle.
     *
     * @param descriptor    descriptor to check
     * @return {@code true} if an instance of the type represented by the descriptor may actively form a cycle
     */
    boolean canParticipateInCycles(ClassDescriptor descriptor) {
        return !builtInNonContainerMarshallers.supports(descriptor.clazz());
    }

    private ClassDescriptor obtainOriginalDescriptor(@Nullable Object object, Class<?> declaredClass) {
        // object class is not a subclass of the declared class for primitives
        // for enums we don't need the specific classes at all
        Class<?> classToQueryForOriginalDescriptor = isInstanceOfSubclass(object, declaredClass)
                && !(object instanceof Enum)
                ? object.getClass() : declaredClass;

        return getOrCreateDescriptor(classToQueryForOriginalDescriptor);
    }

    private boolean objectIsMemberOfEnumWithAnonymousClassesForMembers(Object object, Class<?> declaredClass) {
        return declaredClass.isEnum() && object.getClass().getSuperclass() == declaredClass;
    }

    private void throwIfMarshallingNotSupported(@Nullable Object object) {
        if (object == null) {
            return;
        }
        if (Enum.class.isAssignableFrom(object.getClass())) {
            return;
        }

        Class<?> objectClass = object.getClass();
        if (isInnerClass(objectClass)) {
            throw new IllegalArgumentException("Non-static inner class instances are not supported for marshalling: " + objectClass);
        }
        if (isCapturingClosure(objectClass)) {
            throw new IllegalArgumentException("Capturing nested class instances are not supported for marshalling: " + object);
        }
    }

    private boolean isInnerClass(Class<?> objectClass) {
        return objectClass.getDeclaringClass() != null && !Modifier.isStatic(objectClass.getModifiers());
    }

    private boolean isCapturingClosure(Class<?> objectClass) {
        for (Field field : objectClass.getDeclaredFields()) {
            if ((field.isSynthetic() && field.getName().equals("this$0"))
                    || field.getName().startsWith("arg$")) {
                return true;
            }
        }

        return false;
    }

    private DescribedObject applyWriteReplaceIfNeeded(@Nullable Object objectBefore, ClassDescriptor descriptorBefore)
            throws MarshalException {
        if (!descriptorBefore.supportsWriteReplace()) {
            return new DescribedObject(objectBefore, descriptorBefore);
        }

        Object replacedObject = applyWriteReplace(objectBefore, descriptorBefore);
        ClassDescriptor replacementDescriptor = getOrCreateDescriptor(replacedObject, objectClass(replacedObject));

        if (descriptorBefore.describesSameClass(replacementDescriptor)) {
            return new DescribedObject(replacedObject, replacementDescriptor);
        } else {
            return applyWriteReplaceIfNeeded(replacedObject, replacementDescriptor);
        }
    }

    private boolean isInstanceOfSubclass(@Nullable Object object, Class<?> maybeSuperclass) {
        return object != null && maybeSuperclass.isAssignableFrom(object.getClass());
    }

    @Nullable
    private Object applyWriteReplace(Object originalObject, ClassDescriptor originalDescriptor) throws MarshalException {
        try {
            return originalDescriptor.serializationMethods().writeReplace(originalObject);
        } catch (SpecialMethodInvocationException e) {
            throw new MarshalException("Cannot apply writeReplace()", e);
        }
    }

    private ClassDescriptor getOrCreateDescriptor(@Nullable Object object, Class<?> declaredClass) {
        assert object != null || declaredClass == Null.class;

        if (object == null) {
            return descriptorRegistry.getNullDescriptor();
        }

        return getOrCreateDescriptor(object.getClass());
    }

    private ClassDescriptor getOrCreateDescriptor(Class<?> objectClass) {
        // ENUM and ENUM_ARRAY need to be handled separately because an enum value usually has a class different from
        // Enum and an ENUM_ARRAY might be used for both Enum[] and EnumSubclass[].
        if (objectClass.isEnum()) {
            return descriptorRegistry.getEnumDescriptor();
        }
        if (isEnumArray(objectClass)) {
            return descriptorRegistry.getRequiredDescriptor(Enum[].class);
        }

        ClassDescriptor descriptor = descriptorRegistry.getDescriptor(objectClass);
        if (descriptor != null) {
            return descriptor;
        } else {
            // This is some custom class (not a built-in). If it's a non-built-in array, we need handle it as a generic container.
            if (objectClass.isArray()) {
                return descriptorRegistry.getBuiltInDescriptor(BuiltinType.OBJECT_ARRAY);
            }

            return descriptorFactory.create(objectClass);
        }
    }

    private boolean isEnumArray(Class<?> objectClass) {
        return objectClass.isArray() && objectClass.getComponentType().isEnum();
    }

    private void writeReference(int referenceId, DataOutput output) throws IOException {
        writeDescriptorOrCommandId(SerializedStreamCommands.REFERENCE, output);
        ProtocolMarshalling.writeObjectId(referenceId, output);
    }

    private void marshalCycleable(DescribedObject describedObject, DataOutputStream output, MarshallingContext context)
            throws IOException, MarshalException {
        writeDescriptorId(describedObject.descriptor, output);
        ProtocolMarshalling.writeObjectId(context.referenceId(describedObject.object), output);

        writeObject(describedObject.object, describedObject.descriptor, output, context);
    }

    private void marshalNonCycleable(DescribedObject describedObject, DataOutputStream output, MarshallingContext context)
            throws IOException, MarshalException {
        writeDescriptorId(describedObject.descriptor, output);

        writeObject(describedObject.object, describedObject.descriptor, output, context);
    }

    private void writeDescriptorId(ClassDescriptor descriptor, DataOutput output) throws IOException {
        writeDescriptorOrCommandId(descriptor.descriptorId(), output);
    }

    private void writeDescriptorOrCommandId(int id, DataOutput output) throws IOException {
        ProtocolMarshalling.writeDescriptorOrCommandId(id, output);
    }

    private void writeObject(@Nullable Object object, ClassDescriptor descriptor, DataOutputStream output, MarshallingContext context)
            throws IOException, MarshalException {
        if (isBuiltInNonContainer(descriptor)) {
            builtInNonContainerMarshallers.writeBuiltIn(object, descriptor, output, context);
        } else if (isBuiltInCollection(descriptor)) {
            builtInContainerMarshallers.writeBuiltInCollection((Collection<?>) object, descriptor, output, context);
        } else if (isBuiltInMap(descriptor)) {
            builtInContainerMarshallers.writeBuiltInMap((Map<?, ?>) object, descriptor, output, context);
        } else if (isArray(descriptor)) {
            //noinspection ConstantConditions
            builtInContainerMarshallers.writeGenericRefArray((Object[]) object, descriptor, output, context);
        } else if (descriptor.isExternalizable()) {
            externalizableMarshaller.writeExternalizable((Externalizable) object, descriptor, output, context);
        } else {
            arbitraryObjectMarshaller.writeArbitraryObject(object, descriptor, output, context);
        }
    }

    private boolean isBuiltInNonContainer(ClassDescriptor descriptor) {
        return descriptor.isBuiltIn() && builtInNonContainerMarshallers.supports(descriptor.clazz());
    }

    private boolean isArray(ClassDescriptor descriptor) {
        return descriptor.clazz().isArray();
    }

    private boolean isBuiltInCollection(ClassDescriptor descriptor) {
        return descriptor.isBuiltIn() && Collection.class.isAssignableFrom(descriptor.clazz());
    }

    private boolean isBuiltInMap(ClassDescriptor descriptor) {
        return descriptor.isBuiltIn() && Map.class.isAssignableFrom(descriptor.clazz());
    }

    /** {@inheritDoc} */
    @Override
    @Nullable
    public <T> T unmarshal(byte[] bytes, IdIndexedDescriptors mergedDescriptors) throws UnmarshalException {
        try (var bais = new ByteArrayInputStream(bytes); var dis = new DataInputStream(bais)) {
            UnmarshallingContext context = new UnmarshallingContext(bais, mergedDescriptors);
            T result = unmarshalFromInput(dis, context);

            throwIfExcessiveBytesRemain(dis);

            return result;
        } catch (IOException e) {
            throw new UnmarshalException("Cannot unmarshal", e);
        }
    }

    private <T> T unmarshalFromInput(DataInputStream input, UnmarshallingContext context) throws IOException, UnmarshalException {
        int commandOrDescriptorId = ProtocolMarshalling.readDescriptorOrCommandId(input);
        if (commandOrDescriptorId == SerializedStreamCommands.REFERENCE) {
            // TODO: IGNITE-16165 - make sure readResolve() is applied correctly when we exit early due to reading a reference
            return unmarshalReference(input, context);
        }

        ClassDescriptor descriptor = context.getRequiredDescriptor(commandOrDescriptorId);
        Object readObject;
        if (canParticipateInCycles(descriptor)) {
            readObject = readCycleable(input, context, descriptor);
        } else {
            readObject = readObject(input, descriptor, context);
        }

        @SuppressWarnings("unchecked") T resolvedObject = (T) applyReadResolveIfNeeded(descriptor, readObject);
        return resolvedObject;
    }

    private <T> T unmarshalReference(DataInput input, UnmarshallingContext context) throws IOException {
        int objectId = ProtocolMarshalling.readObjectId(input);
        return context.dereference(objectId);
    }

    private Object readCycleable(DataInputStream input, UnmarshallingContext context, ClassDescriptor descriptor)
            throws IOException, UnmarshalException {
        int objectId = ProtocolMarshalling.readObjectId(input);

        Object preInstantiatedObject = preInstantiate(descriptor, input, context);
        context.registerReference(objectId, preInstantiatedObject);

        fillObjectFrom(input, preInstantiatedObject, descriptor, context);

        return preInstantiatedObject;
    }

    private Object preInstantiate(ClassDescriptor descriptor, DataInput input, UnmarshallingContext context)
            throws IOException, UnmarshalException {
        if (isBuiltInNonContainer(descriptor)) {
            throw new IllegalStateException("Should not be here");
        } else if (isBuiltInCollection(descriptor)) {
            return builtInContainerMarshallers.preInstantiateBuiltInMutableCollection(descriptor, input, context);
        } else if (isBuiltInMap(descriptor)) {
            return builtInContainerMarshallers.preInstantiateBuiltInMutableMap(descriptor, input, context);
        } else if (isArray(descriptor)) {
            return preInstantiateGenericRefArray(input);
        } else if (descriptor.isExternalizable()) {
            return externalizableMarshaller.preInstantiateExternalizable(descriptor);
        } else {
            return arbitraryObjectMarshaller.preInstantiateArbitraryObject(descriptor);
        }
    }

    private Object[] preInstantiateGenericRefArray(DataInput input) throws IOException {
        return builtInContainerMarshallers.preInstantiateGenericRefArray(input);
    }

    private void fillObjectFrom(DataInputStream input, Object preInstantiatedObject, ClassDescriptor descriptor, UnmarshallingContext context)
            throws UnmarshalException, IOException {
        if (isBuiltInNonContainer(descriptor)) {
            throw new IllegalStateException("Cannot fill " + descriptor.clazz() + ", this is a programmatic error");
        } else if (isBuiltInCollection(descriptor)) {
            fillBuiltInCollectionFrom(input, (Collection<?>) preInstantiatedObject, descriptor, context);
        } else if (isBuiltInMap(descriptor)) {
            fillBuiltInMapFrom(input, (Map<?, ?>) preInstantiatedObject, context);
        } else if (isArray(descriptor)) {
            fillGenericRefArrayFrom(input, (Object[]) preInstantiatedObject, context);
        } else if (descriptor.isExternalizable()) {
            externalizableMarshaller.fillExternalizableFrom(input, (Externalizable) preInstantiatedObject, context);
        } else {
            arbitraryObjectMarshaller.fillArbitraryObjectFrom(input, preInstantiatedObject, descriptor, context);
        }
    }

    private void fillBuiltInCollectionFrom(
            DataInputStream input,
            Collection<?> preInstantiatedObject,
            ClassDescriptor descriptor,
            UnmarshallingContext context
    ) throws UnmarshalException, IOException {
        builtInContainerMarshallers.fillBuiltInCollectionFrom(input, preInstantiatedObject, descriptor, this::unmarshalFromInput, context);
    }

    private void fillBuiltInMapFrom(
            DataInputStream input,
            Map<?, ?> preInstantiatedObject,
            UnmarshallingContext context
    ) throws UnmarshalException, IOException {
        builtInContainerMarshallers.fillBuiltInMapFrom(input,
                preInstantiatedObject,
                this::unmarshalFromInput,
                this::unmarshalFromInput,
                context
        );
    }

    private void fillGenericRefArrayFrom(DataInputStream input, Object[] array, UnmarshallingContext context)
            throws IOException, UnmarshalException {
        builtInContainerMarshallers.fillGenericRefArray(input, array, this::unmarshalFromInput, context);
    }

    @Nullable
    private Object readObject(DataInputStream input, ClassDescriptor descriptor, UnmarshallingContext context)
            throws IOException, UnmarshalException {
        if (isBuiltInNonContainer(descriptor)) {
            return builtInNonContainerMarshallers.readBuiltIn(descriptor, input, context);
        } else {
            throw new IllegalStateException("Cannot read an instance of " + descriptor.clazz() + ", this is a programmatic error");
        }
    }

    private Object applyReadResolveIfNeeded(ClassDescriptor descriptor, Object object) throws UnmarshalException {
        if (descriptor.hasReadResolve()) {
            return applyReadResolve(descriptor, object);
        } else {
            return object;
        }
    }

    private Object applyReadResolve(ClassDescriptor descriptor, Object readObject) throws UnmarshalException {
        try {
            return descriptor.serializationMethods().readResolve(readObject);
        } catch (SpecialMethodInvocationException e) {
            throw new UnmarshalException("Cannot apply readResolve()", e);
        }
    }

    private void throwIfExcessiveBytesRemain(DataInputStream dis) throws IOException, UnmarshalException {
        if (dis.available() > 0) {
            throw new UnmarshalException("After reading a value, " + dis.available() + " excessive byte(s) still remain");
        }
    }

    private static class DescribedObject {
        @Nullable
        private final Object object;
        private final ClassDescriptor descriptor;

        private DescribedObject(@Nullable Object object, ClassDescriptor descriptor) {
            this.object = object;
            this.descriptor = descriptor;
        }
    }
}
