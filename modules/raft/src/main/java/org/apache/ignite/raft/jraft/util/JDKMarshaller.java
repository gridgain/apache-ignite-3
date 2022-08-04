/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.ignite.raft.jraft.util;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import jdk.jfr.Event;
import jdk.jfr.Label;
import jdk.jfr.Name;

/**
 *
 */
public class JDKMarshaller implements Marshaller {
    @Name("MarshallDuration")
    @Label("Marshall")
    static class MarshallDuration extends Event { }

    @Name("UnmarshallDuration")
    @Label("Unmarshall")
    static class Unmarshall extends Event { }

    /**
     * {@inheritDoc}
     */
    @Override public byte[] marshall(Object o) {
        MarshallDuration event = new MarshallDuration();
        event.begin();
        try {
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            ObjectOutputStream oos = new ObjectOutputStream(baos);
            oos.writeObject(o);
            oos.close();
            return baos.toByteArray();
        }
        catch (Exception e) {
            throw new Error(e);
        } finally {
            event.commit();
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override public <T> T unmarshall(byte[] raw) {
        Unmarshall event = new Unmarshall();
        event.begin();
        try {
            ByteArrayInputStream bais = new ByteArrayInputStream(raw);
            ObjectInputStream oos = new ObjectInputStream(bais);
            return (T) oos.readObject();
        }
        catch (Exception e) {
            throw new Error(e);
        } finally {
            event.commit();
        }
    }
}
