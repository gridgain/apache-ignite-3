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

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.lessThan;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.Externalizable;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.Objects;
import java.util.Set;
import java.util.function.Consumer;
import java.util.stream.Stream;
import org.apache.ignite.internal.network.serialization.BuiltinType;
import org.apache.ignite.internal.network.serialization.ClassDescriptor;
import org.apache.ignite.internal.network.serialization.ClassDescriptorFactory;
import org.apache.ignite.internal.network.serialization.ClassDescriptorFactoryContext;
import org.apache.ignite.internal.network.serialization.IdIndexedDescriptors;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * Tests for how {@link DefaultUserObjectMarshaller} handles {@link java.io.Serializable}s (but not {@link Externalizable}s).
 */
class DefaultUserObjectMarshallerWithSerializableTest {
    private final ClassDescriptorFactoryContext descriptorRegistry = new ClassDescriptorFactoryContext();
    private final ClassDescriptorFactory descriptorFactory = new ClassDescriptorFactory(descriptorRegistry);
    private final IdIndexedDescriptors descriptors = new ContextBasedIdIndexedDescriptors(descriptorRegistry);

    private final DefaultUserObjectMarshaller marshaller = new DefaultUserObjectMarshaller(descriptorRegistry, descriptorFactory);

    private static final int WRITE_REPLACE_INCREMENT = 1_000_000;
    private static final int READ_RESOLVE_INCREMENT = 1_000;

    private static final int WRITE_OBJECT_INCREMENT = 10;
    private static final int READ_OBJECT_INCREMENT = 100;

    private static final int CHILD_WRITE_OBJECT_INCREMENT = 3;
    private static final int CHILD_READ_OBJECT_INCREMENT = 6;

    /** This is static so that writeObject()/readObject() can easily find it. */
    private static ReaderAndWriter<?> readerAndWriter;

    /** Static access to the marshaller (for using in parameterized tests) */
    private static UserObjectMarshaller staticMarshaller;
    /** Static access to the registry (for using in parameterized tests) */
    private static ClassDescriptorFactoryContext staticDescriptorRegistry;

    private static boolean nonSerializableParentConstructorCalled;
    private static boolean constructorCalled;

    @BeforeEach
    void initStatics() {
        staticMarshaller = marshaller;
        staticDescriptorRegistry = descriptorRegistry;
    }

    @Test
    void marshalsAndUnmarshalsSerializable() throws Exception {
        SimpleSerializable unmarshalled = marshalAndUnmarshalNonNull(new SimpleSerializable(42));

        assertThat(unmarshalled.intValue, is(42));
    }

    private <T> T marshalAndUnmarshalNonNull(Object object) throws MarshalException, UnmarshalException {
        MarshalledObject marshalled = marshaller.marshal(object);
        return unmarshalNonNull(marshalled);
    }

    private <T> T unmarshalNonNull(MarshalledObject marshalled) throws UnmarshalException {
        T unmarshalled = marshaller.unmarshal(marshalled.bytes(), descriptors);

        assertThat(unmarshalled, is(notNullValue()));

        return unmarshalled;
    }

    @Test
    void appliesWriteReplaceOnSerializable() throws Exception {
        SimpleSerializable unmarshalled = marshalAndUnmarshalNonNull(new SerializableWithWriteReplace(42));

        assertThat(unmarshalled.intValue, is(equalTo(42 + WRITE_REPLACE_INCREMENT)));
    }

    @Test
    void appliesReadResolveOnSerializable() throws Exception {
        SimpleSerializable unmarshalled = marshalAndUnmarshalNonNull(new SerializableWithReadResolve(42));

        assertThat(unmarshalled.intValue, is(equalTo(42 + READ_RESOLVE_INCREMENT)));
    }

    @Test
    void appliesBothWriteReplaceAndReadResolveOnSerializable() throws Exception {
        SimpleSerializable unmarshalled = marshalAndUnmarshalNonNull(new SerializableWithWriteReplaceReadResolve(42));

        assertThat(unmarshalled.intValue, is(equalTo(42 + WRITE_REPLACE_INCREMENT + READ_RESOLVE_INCREMENT)));
    }

    @Test
    void usesDescriptorOfReplacementWhenSerializableIsReplacedWithSomethingDifferent() throws Exception {
        MarshalledObject marshalled = marshaller.marshal(new SerializableWithReplaceWithSimple(42));

        ClassDescriptor originalDescriptor = descriptorRegistry.getRequiredDescriptor(SerializableWithReplaceWithSimple.class);
        assertThat(marshalled.usedDescriptors(), not(hasItem(originalDescriptor)));

        ClassDescriptor replacementDescriptor = descriptorRegistry.getRequiredDescriptor(SimpleSerializable.class);
        assertThat(marshalled.usedDescriptors(), hasItem(replacementDescriptor));
    }

    @Test
    void marshalsSerializableWithReplaceWithNull() throws Exception {
        MarshalledObject marshalled = marshaller.marshal(new SerializableWithReplaceWithNull(42));

        SimpleSerializable unmarshalled = marshaller.unmarshal(marshalled.bytes(), descriptors);

        assertThat(unmarshalled, is(nullValue()));
    }

    @Test
    void onlyUsesDescriptorOfNullWhenSerializableIsReplacedWithNull() throws Exception {
        MarshalledObject marshalled = marshaller.marshal(new SerializableWithReplaceWithNull(42));

        ClassDescriptor replacementDescriptor = descriptorRegistry.getNullDescriptor();
        assertThat(marshalled.usedDescriptors(), equalTo(Set.of(replacementDescriptor)));
    }

    @Test
    void unmarshalsSerializableWithResolveWithNull() throws Exception {
        MarshalledObject marshalled = marshaller.marshal(new SerializableWithResolveWithNull(42));

        SimpleSerializable unmarshalled = marshaller.unmarshal(marshalled.bytes(), descriptors);

        assertThat(unmarshalled, is(nullValue()));
    }

    @Test
    void appliesWriteReplaceOnExternalizableRecursively() throws Exception {
        Object result = marshalAndUnmarshalNonNull(new SerializableWithWriteReplaceChain1(0));

        assertThat(result, is(instanceOf(Integer.class)));
        assertThat(result, is(3));
    }

    @Test
    void stopsApplyingWriteReplaceOnExternalizableWhenReplacementIsInstanceOfSameClass() throws Exception {
        SerializableWithWriteReplaceWithSameClass result = marshalAndUnmarshalNonNull(new SerializableWithWriteReplaceWithSameClass(0));

        assertThat(result.intValue, is(1));
    }

    @Test
    void causesInfiniteRecursionOnExternalizableWithIndirectWriteReplaceCycle() {
        assertThrows(StackOverflowError.class, ()  -> marshalAndUnmarshalNonNull(new SerializableWithWriteReplaceCycle1(0)));
    }

    /**
     * Java Serialization applies writeReplace() repeatedly, but it only applies readResolve() once.
     * So we are emulating this behavior.
     *
     * @throws Exception if something goes wrong
     */
    @Test
    void onlyAppliesFirstReadResolveOnExternalizable() throws Exception {
        Object result = marshalAndUnmarshalNonNull(new SerializableWithReadResolveChain1(0));

        assertThat(result, is(instanceOf(SerializableWithReadResolveChain2.class)));
    }

    @Test
    void usesWriteObjectAndReadObject() throws Exception {
        SerializableWithWriteReadOverride result = marshalAndUnmarshalNonNull(new SerializableWithWriteReadOverride(42));

        assertThat(result.value, is(42 + WRITE_OBJECT_INCREMENT + READ_OBJECT_INCREMENT));
    }

    @Test
    void doesNotWriteDefaultFieldValuesDataIfWriteReadOverrideIsPresent() throws Exception {
        SerializableWithNoOpWriteReadOverride result = marshalAndUnmarshalNonNull(new SerializableWithNoOpWriteReadOverride(42));

        assertThat(result.value, is(0));
    }

    @ParameterizedTest
    @MethodSource("readWriteSpecs")
    <T> void objectOutputStreamFromWriteObjectWritesUsingOurFormat(ReadWriteSpec<T> spec) throws Exception {
        readerAndWriter = new ReaderAndWriter<>(spec.writer, spec.reader);

        WithCustomizableOverride<T> original = new WithCustomizableOverride<>();
        MarshalledObject marshalled = marshaller.marshal(original);

        byte[] overrideBytes = readOverrideBytes(marshalled);
        T overrideValue = spec.parseOverrideValue(overrideBytes);

        spec.assertUnmarshalledValue(overrideValue);
    }

    @ParameterizedTest
    @MethodSource("readWriteSpecs")
    <T> void supportsReadsAndWritesInWriteObjectAndReadObject(ReadWriteSpec<T> spec) throws Exception {
        readerAndWriter = new ReaderAndWriter<>(spec.writer, spec.reader);

        WithCustomizableOverride<T> original = new WithCustomizableOverride<>();
        WithCustomizableOverride<T> unmarshalled = marshalAndUnmarshalNonNull(original);

        spec.assertUnmarshalledValue(unmarshalled.value);
    }

    private byte[] readOverrideBytes(MarshalledObject marshalled) throws IOException {
        DataInputStream dis = new DataInputStream(new ByteArrayInputStream(marshalled.bytes()));

        ProtocolMarshalling.readDescriptorOrCommandId(dis);
        ProtocolMarshalling.readObjectId(dis);

        return dis.readAllBytes();
    }

    private void assertThatDrained(DataInputStream dis) throws IOException {
        assertThat(dis.read(), is(lessThan(0)));
    }

    private static Stream<Arguments> readWriteSpecs() {
        return Stream.of(
                // the following test perfect pairs like writeByte()/readByte()
                new ReadWriteSpec<>("data byte", oos -> oos.writeByte(42), DataInput::readByte, (byte) 42),
                new ReadWriteSpec<>("short", oos -> oos.writeShort(42), DataInput::readShort, (short) 42),
                new ReadWriteSpec<>("int", oos -> oos.writeInt(42), DataInput::readInt, 42),
                new ReadWriteSpec<>("long", oos -> oos.writeLong(42), DataInput::readLong, 42L),
                new ReadWriteSpec<>("float", oos -> oos.writeFloat(42.0f), DataInput::readFloat, 42.0f),
                new ReadWriteSpec<>("double", oos -> oos.writeDouble(42.0), DataInput::readDouble, 42.0),
                new ReadWriteSpec<>("char", oos -> oos.writeChar('a'), DataInput::readChar, 'a'),
                new ReadWriteSpec<>("boolean", oos -> oos.writeBoolean(true), DataInput::readBoolean, true),
                new ReadWriteSpec<>("stream byte", oos -> oos.write(42), ObjectInputStream::read, DataInputStream::read, 42),
                new ReadWriteSpec<>("byte array", oos -> oos.write(new byte[]{42, 43}), is -> readBytes(is, 2), is -> readBytes(is, 2), new byte[]{42, 43}),
                new ReadWriteSpec<>("byte array range", oos -> oos.write(new byte[]{42, 43}, 0, 2), is -> readRange(is, 2), is -> readRange(is, 2), new byte[]{42, 43}),
                new ReadWriteSpec<>("UTF", oos -> oos.writeUTF("Привет"), DataInput::readUTF, "Привет"),
                new ReadWriteSpec<>("object", oos -> oos.writeObject(new SimpleNonSerializable(42)), ObjectInputStream::readObject, DefaultUserObjectMarshallerWithSerializableTest::consumeAndUnmarshal, new SimpleNonSerializable(42)),
                new ReadWriteSpec<>("unshared", oos -> oos.writeUnshared(new SimpleNonSerializable(42)), ObjectInputStream::readUnshared, DefaultUserObjectMarshallerWithSerializableTest::consumeAndUnmarshal, new SimpleNonSerializable(42)),
                // the following test writing methods only (readers are just to help testing them)
                new ReadWriteSpec<>("writeBytes", oos -> oos.writeBytes("abc"), input -> readBytesFully(input, 3), "abc".getBytes()),
                new ReadWriteSpec<>("writeChars", oos -> oos.writeChars("a"), DataInput::readChar, 'a'),
                // the following test reading methods only (writers are just to help testing them)
                new ReadWriteSpec<>("readFully", oos -> oos.write(new byte[]{42, 43}), input -> readBytesFully(input, 2), new byte[]{42, 43}),
                new ReadWriteSpec<>("readFully range", oos -> oos.write(new byte[]{42, 43}), input -> readBytesRangeFully(input, 2), new byte[]{42, 43}),
                new ReadWriteSpec<>("readUnsignedByte", oos -> oos.writeByte(42), DataInput::readUnsignedByte, 42),
                new ReadWriteSpec<>("readUnsignedShort", oos -> oos.writeShort(42), DataInput::readUnsignedShort, 42),
                new ReadWriteSpec<>("readAllBytes", oos -> oos.write(new byte[]{42, 43}), InputStream::readAllBytes, InputStream::readAllBytes, new byte[]{42, 43}),
                new ReadWriteSpec<>("readNBytes", oos -> oos.write(new byte[]{42, 43}), ois -> ois.readNBytes(2), dis -> readBytesFully(dis, 2), new byte[]{42, 43}),
                new ReadWriteSpec<>("readNBytes range", oos -> oos.write(new byte[]{42, 43}), ois -> readNBytesRange(ois, 2), dis -> readNBytesRange(dis, 2), new byte[]{42, 43})
        ).map(Arguments::of);
    }

    // TODO: IGNITE-16240 - implement putFields()/writeFields()
    // TODO: IGNITE-16240 - implement readFields()

    private static byte[] readBytes(InputStream is, int count) throws IOException {
        byte[] bytes = new byte[count];
        int read = is.read(bytes);
        assertThat(read, is(count));
        return bytes;
    }

    private static byte[] readRange(InputStream is, int count) throws IOException {
        byte[] bytes = new byte[count];
        int read = is.read(bytes, 0, count);
        assertThat(read, is(count));
        return bytes;
    }

    private static byte[] readBytesFully(DataInput is, int count) throws IOException {
        byte[] bytes = new byte[count];
        is.readFully(bytes);
        return bytes;
    }

    private static byte[] readBytesRangeFully(DataInput is, int count) throws IOException {
        byte[] bytes = new byte[count];
        is.readFully(bytes, 0, count);
        return bytes;
    }

    private static byte[] readNBytesRange(InputStream is, int count) throws IOException {
        byte[] bytes = new byte[count];
        is.readNBytes(bytes, 0, count);
        return bytes;
    }

    private static <T> T consumeAndUnmarshal(DataInputStream stream) throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        stream.transferTo(baos);

        try {
            return staticMarshaller.unmarshal(baos.toByteArray(), new ContextBasedIdIndexedDescriptors(staticDescriptorRegistry));
        } catch (UnmarshalException e) {
            throw new RuntimeException("Unmarshalling failed", e);
        }
    }

    @Test
    void supportsFlushInsideWriteObject() {
        readerAndWriter = new ReaderAndWriter<>(ObjectOutputStream::flush, ois -> null);

        WithCustomizableOverride<?> original = new WithCustomizableOverride<>();

        assertDoesNotThrow(() -> marshalAndUnmarshalNonNull(original));
    }

    @Test
    void supportsResetInsideWriteObject() {
        readerAndWriter = new ReaderAndWriter<>(ObjectOutputStream::reset, ois -> null);

        WithCustomizableOverride<?> original = new WithCustomizableOverride<>();

        assertDoesNotThrow(() -> marshalAndUnmarshalNonNull(original));
    }

    @Test
    void supportsUseProtocolVersionInsideWriteObject() {
        readerAndWriter = new ReaderAndWriter<>(oos -> oos.useProtocolVersion(1), ois -> null);

        WithCustomizableOverride<?> original = new WithCustomizableOverride<>();

        assertDoesNotThrow(() -> marshalAndUnmarshalNonNull(original));
    }

    @Test
    void supportsSkipInsideReadObject() throws Exception {
        readerAndWriter = new ReaderAndWriter<>(oos -> oos.write(new byte[]{42, 43}), ois -> {
            assertThat(ois.skip(1), is(1L));
            return ois.readByte();
        });

        WithCustomizableOverride<?> original = new WithCustomizableOverride<>();

        WithCustomizableOverride<Byte> unmarshalled = marshalAndUnmarshalNonNull(original);
        assertThat(unmarshalled.value, is((byte) 43));
    }

    @Test
    void supportsSkipBytesInsideReadObject() throws Exception {
        readerAndWriter = new ReaderAndWriter<>(oos -> oos.write(new byte[]{42, 43}), ois -> {
            assertThat(ois.skipBytes(1), is(1));
            return ois.readByte();
        });

        WithCustomizableOverride<?> original = new WithCustomizableOverride<>();

        WithCustomizableOverride<Byte> unmarshalled = marshalAndUnmarshalNonNull(original);
        assertThat(unmarshalled.value, is((byte) 43));
    }

    @Test
    void supportsAvailableInsideReadObject() {
        readerAndWriter = new ReaderAndWriter<>(oos -> {}, ObjectInputStream::available);

        WithCustomizableOverride<?> original = new WithCustomizableOverride<>();

        assertDoesNotThrow(() -> marshalAndUnmarshalNonNull(original));
    }

    @Test
    void supportsMarkAndResetInsideReadObject() {
        readerAndWriter = new ReaderAndWriter<>(oos -> {}, ois -> {
            //noinspection ResultOfMethodCallIgnored
            assertFalse(ois.markSupported());
            ois.mark(1);
            try {
                ois.reset();
            } catch (IOException e) {
                // ignore mark/reset not supported
            }
            return null;
        });

        WithCustomizableOverride<?> original = new WithCustomizableOverride<>();

        assertDoesNotThrow(() -> marshalAndUnmarshalNonNull(original));
    }

    @Test
    void defaultWriteObjectFromWriteObjectWritesUsingOurFormat() throws Exception {
        WithOverrideStillUsingDefaultMechanism original = new WithOverrideStillUsingDefaultMechanism(42);
        MarshalledObject marshalled = marshaller.marshal(original);

        byte[] overrideBytes = readOverrideBytes(marshalled);
        DataInputStream dis = new DataInputStream(new ByteArrayInputStream(overrideBytes));

        assertThat(ProtocolMarshalling.readDescriptorOrCommandId(dis), is(BuiltinType.INT.descriptorId()));
        assertThat(dis.readInt(), is(42));
        assertThatDrained(dis);
    }

    @Test
    void marshalsAndUnmarshalsSerializableWithReadWriteObjectUsingDefaultMechanism() throws Exception {
        WithOverrideStillUsingDefaultMechanism result = marshalAndUnmarshalNonNull(new WithOverrideStillUsingDefaultMechanism(42));

        assertThat(result.value, is(42));
    }

    @Test
    void marshalsAndUnmarshalsNestedSerializablesWithReadWriteObjectUsingDefaultMechanism() throws Exception {
        NestHostUsingDefaultMechanism result = marshalAndUnmarshalNonNull(
                new NestHostUsingDefaultMechanism(42, new NestedUsingDefaultMechanism(100))
        );

        assertThat(result.value, is(42));
        assertThat(result.nested.value, is(100));
    }

    @Test
    void supportsWriteObjectAndReadObjectInHierarchy() throws Exception {
        SubclassWithWriteReadOverride result = marshalAndUnmarshalNonNull(new SubclassWithWriteReadOverride(42));

        assertThat(((SerializableWithWriteReadOverride) result).value, is(42 + WRITE_OBJECT_INCREMENT + READ_OBJECT_INCREMENT));
        assertThat(result.childValue, is(42 + CHILD_WRITE_OBJECT_INCREMENT + CHILD_READ_OBJECT_INCREMENT));
    }

    @Test
    void invokesNoArgConstructorOfNonSerializableParentOnUnmarshalling() throws Exception {
        SerializableWithSideEffectInParentConstructor object = new SerializableWithSideEffectInParentConstructor();
        nonSerializableParentConstructorCalled = false;

        marshalAndUnmarshalNonNull(object);

        assertTrue(nonSerializableParentConstructorCalled);
    }

    @Test
    void doesNotInvokeNoArgConstructorOfSerializableOnUnmarshalling() throws Exception {
        SerializableWithSideEffectInConstructor object = new SerializableWithSideEffectInConstructor();
        constructorCalled = false;

        marshalAndUnmarshalNonNull(object);

        assertFalse(constructorCalled);
    }

    /**
     * An {@link Serializable} that does not have {@code writeReplace()}/{@code readResolve()} methods or other customizations.
     */
    private static class SimpleSerializable implements Serializable {
        int intValue;

        public SimpleSerializable(int intValue) {
            this.intValue = intValue;
        }
    }

    private static class SerializableWithWriteReplace extends SimpleSerializable {
        public SerializableWithWriteReplace(int intValue) {
            super(intValue);
        }

        private Object writeReplace() {
            return new SerializableWithWriteReplace(intValue + 1_000_000);
        }
    }

    private static class SerializableWithReadResolve extends SimpleSerializable {
        public SerializableWithReadResolve(int intValue) {
            super(intValue);
        }

        private Object readResolve() {
            return new SerializableWithReadResolve(intValue + 1_000);
        }
    }

    private static class SerializableWithWriteReplaceReadResolve extends SimpleSerializable {
        public SerializableWithWriteReplaceReadResolve(int intValue) {
            super(intValue);
        }

        private Object writeReplace() {
            return new SerializableWithWriteReplaceReadResolve(intValue + 1_000_000);
        }

        private Object readResolve() {
            return new SerializableWithWriteReplaceReadResolve(intValue + 1_000);
        }
    }

    private static class SerializableWithReplaceWithSimple implements Serializable {
        private final int intValue;

        public SerializableWithReplaceWithSimple(int intValue) {
            this.intValue = intValue;
        }

        private Object writeReplace() {
            return new SimpleSerializable(intValue);
        }
    }

    private static class SerializableWithReplaceWithNull extends SimpleSerializable {
        public SerializableWithReplaceWithNull(int intValue) {
            super(intValue);
        }

        private Object writeReplace() {
            return null;
        }
    }

    private static class SerializableWithResolveWithNull extends SimpleSerializable {
        public SerializableWithResolveWithNull(int intValue) {
            super(intValue);
        }

        private Object readResolve() {
            return null;
        }
    }

    private static class SerializableWithWriteReplaceChain1 extends SimpleSerializable {
        public SerializableWithWriteReplaceChain1(int value) {
            super(value);
        }

        private Object writeReplace() {
            return new SerializableWithWriteReplaceChain2(intValue + 1);
        }
    }

    private static class SerializableWithWriteReplaceChain2 extends SimpleSerializable {
        public SerializableWithWriteReplaceChain2(int value) {
            super(value);
        }

        private Object writeReplace() {
            return intValue + 2;
        }
    }

    private static class SerializableWithWriteReplaceWithSameClass extends SimpleSerializable {
        public SerializableWithWriteReplaceWithSameClass(int value) {
            super(value);
        }

        private Object writeReplace() {
            return new SerializableWithWriteReplaceWithSameClass(intValue + 1);
        }
    }

    private static class SerializableWithWriteReplaceCycle1 extends SimpleSerializable {
        public SerializableWithWriteReplaceCycle1(int intValue) {
            super(intValue);
        }

        private Object writeReplace() {
            return new SerializableWithWriteReplaceCycle2(intValue);
        }
    }

    private static class SerializableWithWriteReplaceCycle2 extends SimpleSerializable {
        public SerializableWithWriteReplaceCycle2(int intValue) {
            super(intValue);
        }

        private Object writeReplace() {
            return new SerializableWithWriteReplaceCycle1(intValue);
        }
    }

    private static class SerializableWithReadResolveChain1 extends SimpleSerializable {
        public SerializableWithReadResolveChain1(int value) {
            super(value);
        }

        private Object readResolve() {
            return new SerializableWithReadResolveChain2(intValue + 1);
        }
    }

    private static class SerializableWithReadResolveChain2 extends SimpleSerializable {
        public SerializableWithReadResolveChain2(int value) {
            super(value);
        }

        private Object readResolve() {
            return intValue + 2;
        }
    }

    private static class SerializableWithWriteReadOverride implements Serializable {
        private int value;

        public SerializableWithWriteReadOverride() {
        }

        public SerializableWithWriteReadOverride(int value) {
            this.value = value;
        }

        private void writeObject(ObjectOutputStream oos) throws IOException {
            oos.writeInt(value + WRITE_OBJECT_INCREMENT);
        }

        private void readObject(ObjectInputStream ois) throws IOException {
            value = ois.readInt() + READ_OBJECT_INCREMENT;
        }
    }

    private static class SerializableWithNoOpWriteReadOverride implements Serializable {
        private int value;

        public SerializableWithNoOpWriteReadOverride() {
        }

        public SerializableWithNoOpWriteReadOverride(int value) {
            this.value = value;
        }

        private void writeObject(ObjectOutputStream oos) throws IOException {
            // no-op
        }

        private void readObject(ObjectInputStream ois) throws IOException {
            // no-op
        }
    }

    private interface ContentsWriter {
        void writeTo(ObjectOutputStream stream) throws IOException;
    }

    private interface ContentsReader<T> {
        T readFrom(ObjectInputStream stream) throws IOException, ClassNotFoundException;
    }

    private interface DataReader<T> {
        T readFrom(DataInputStream stream) throws IOException;
    }

    private interface InputReader<T> {
        T readFrom(DataInput input) throws IOException;
    }

    private static class ReaderAndWriter<T> {
        private final ContentsWriter writer;
        private final ContentsReader<T> reader;

        private ReaderAndWriter(ContentsWriter writer, ContentsReader<T> reader) {
            this.writer = writer;
            this.reader = reader;
        }
    }

    private static class ReadWriteSpec<T> {
        private final String name;
        private final ContentsWriter writer;
        private final ContentsReader<T> reader;
        private final DataReader<T> dataReader;
        private final Consumer<T> valueAsserter;

        private ReadWriteSpec(String name, ContentsWriter writer, InputReader<T> inputReader, T expectedValue) {
            this(name, writer, inputReader::readFrom, inputReader::readFrom, expectedValue);
        }

        private ReadWriteSpec(String name, ContentsWriter writer, ContentsReader<T> reader, DataReader<T> dataReader, T expectedValue) {
            this(name, writer, reader, dataReader, actual -> assertThat(actual, is(expectedValue)));
        }

        private ReadWriteSpec(String name, ContentsWriter writer, ContentsReader<T> reader, DataReader<T> dataReader, Consumer<T> valueAsserter) {
            this.name = name;
            this.writer = writer;
            this.reader = reader;
            this.dataReader = dataReader;
            this.valueAsserter = valueAsserter;
        }

        private T parseOverrideValue(byte[] overrideBytes) throws IOException {
            DataInputStream dis = new DataInputStream(new ByteArrayInputStream(overrideBytes));
            return dataReader.readFrom(dis);
        }

        private void assertUnmarshalledValue(T value) {
            valueAsserter.accept(value);
        }

        /** {@inheritDoc} */
        @Override
        public String toString() {
            return "ReadWriteSpec (" + name + ")";
        }
    }

    private static class WithCustomizableOverride<T> implements Serializable {
        private T value;

        private void writeObject(ObjectOutputStream oos) throws IOException {
            readerAndWriter.writer.writeTo(oos);
        }

        @SuppressWarnings("unchecked")
        private void readObject(ObjectInputStream ois) throws IOException, ClassNotFoundException {
            value = (T) readerAndWriter.reader.readFrom(ois);
        }
    }

    private static class SimpleNonSerializable {
        private int value;

        @SuppressWarnings("unused")
        public SimpleNonSerializable() {
        }

        public SimpleNonSerializable(int value) {
            this.value = value;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            SimpleNonSerializable that = (SimpleNonSerializable) o;
            return value == that.value;
        }

        @Override
        public int hashCode() {
            return Objects.hash(value);
        }
    }

    private static class WithOverrideStillUsingDefaultMechanism implements Serializable {
        private final int value;

        private WithOverrideStillUsingDefaultMechanism(int value) {
            this.value = value;
        }

        private void writeObject(ObjectOutputStream stream) throws IOException {
            stream.defaultWriteObject();
        }

        private void readObject(ObjectInputStream stream) throws IOException, ClassNotFoundException {
            stream.defaultReadObject();
        }
    }

    private static class NestedUsingDefaultMechanism implements Serializable {
        private final int value;

        private NestedUsingDefaultMechanism(int value) {
            this.value = value;
        }

        private void writeObject(ObjectOutputStream stream) throws IOException {
            stream.defaultWriteObject();
        }

        private void readObject(ObjectInputStream stream) throws IOException, ClassNotFoundException {
            stream.defaultReadObject();
        }
    }

    private static class NestHostUsingDefaultMechanism implements Serializable {
        private final int value;
        private final NestedUsingDefaultMechanism nested;

        private NestHostUsingDefaultMechanism(int value, NestedUsingDefaultMechanism nested) {
            this.value = value;
            this.nested = nested;
        }

        private void writeObject(ObjectOutputStream stream) throws IOException {
            stream.defaultWriteObject();
        }

        private void readObject(ObjectInputStream stream) throws IOException, ClassNotFoundException {
            stream.defaultReadObject();
        }
    }

    private static class SubclassWithWriteReadOverride extends SerializableWithWriteReadOverride {
        private int childValue;

        public SubclassWithWriteReadOverride() {
        }

        public SubclassWithWriteReadOverride(int value) {
            super(value);
            this.childValue = value;
        }

        private void writeObject(ObjectOutputStream oos) throws IOException {
            oos.writeInt(childValue + CHILD_WRITE_OBJECT_INCREMENT);
        }

        private void readObject(ObjectInputStream ois) throws IOException {
            childValue = ois.readInt() + CHILD_READ_OBJECT_INCREMENT;
        }
    }

    private static class NonSerializableParentWithSideEffectInConstructor {
        protected NonSerializableParentWithSideEffectInConstructor() {
            nonSerializableParentConstructorCalled = true;
        }
    }

    private static class SerializableWithSideEffectInParentConstructor extends NonSerializableParentWithSideEffectInConstructor
            implements Serializable {
    }

    private static class SerializableWithSideEffectInConstructor implements Serializable {
        public SerializableWithSideEffectInConstructor() {
            constructorCalled = true;
        }
    }
}
