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

package org.apache.ignite.storage.usecases;

import java.math.BigDecimal;
import org.apache.ignite.storage.binary.BinaryObject;
import org.apache.ignite.storage.KVView;
import org.apache.ignite.storage.Table;
import org.apache.ignite.storage.Row;
import org.apache.ignite.storage.TableView;
import org.apache.ignite.storage.binary.BinaryObjects;
import org.apache.ignite.storage.mapper.Mappers;

/**
 *
 */
@SuppressWarnings({"unused", "UnusedAssignment"})
public class Example {
    /**
     * Use case 1: a simple one. The table has the structure
     * [
     *     [id int, orgId int] // key
     *     [name varchar, lastName varchar, decimal salary, int department] // value
     * ]
     * We show how to use the raw TableRow and a mapped class.
     */
    public void useCase1(Table t) {
        // Search row will allow nulls even in non-null columns.
        Row res = t.get(t.createSearchRow(1, 1));

        String name = res.field("name");
        String lastName = res.field("latName");
        BigDecimal salary = res.field("salary");
        Integer department = res.field("department");

        // We may have primitive-returning methods if needed.
        int departmentPrimitive = res.intField("department");

        // Note that schema itself already defined which fields are key field.
        class Employee {
            final int id;
            final int orgId;

            String name;
            String lastName;
            BigDecimal salary;
            int department;

            Employee(int id, int orgId) {
                this.id = id;
                this.orgId = orgId;
            }
        }

        TableView<Employee> employeeView = t.tableView(Mappers.ofRowClass(Employee.class));

        Employee e = employeeView.get(new Employee(1, 1));

        // As described in the IEP, we can have a truncated mapping.
        class TruncatedEmployee {
            final int id;
            final int orgId;

            String name;
            String lastName;

            TruncatedEmployee(int id, int orgId) {
                this.id = id;
                this.orgId = orgId;
            }
        }

        TableView<TruncatedEmployee> truncatedEmployeeView = t.tableView(Mappers.ofRowClass(TruncatedEmployee.class));

        // salary and department will not be sent over the network during this call.
        TruncatedEmployee te = truncatedEmployeeView.get(new TruncatedEmployee(1, 1));
    }

    /**
     * Use case 2: using byte[] and binary objects in columns.
     * The table has structure
     * [
     *     [id int, orgId int] // key
     *     [originalObject byte[], upgradedObject byte[], int department] // value
     * ]
     * Where {@code originalObject} is some value that was originally put to the column,
     * {@code upgradedObject} is a version 2 of the object, and department is extracted field.
     */
    public void useCase2(Table t) {
        Row res = t.get(t.createSearchRow(1, 1));

        byte[] objData = res.field("originalObject");
        BinaryObject binObj = BinaryObjects.wrap(objData);
        // Work with the binary object as in Ignite 2.x

        // Additionally, we may have a shortcut similar to primitive methods.
        binObj = res.binaryObjectField("upgradedObject");

        // Plain byte[] and BinaryObject fields in a class are straightforward.
        class Record {
            final int id;
            final int orgId;

            byte[] originalObject;
            BinaryObject upgradedObject;
            int department;

            Record(int id, int orgId) {
                this.id = id;
                this.orgId = orgId;
            }
        }

        TableView<Record> recordView = t.tableView(Mappers.ofRowClass(Record.class));

        // Similarly work with the binary objects.
        Record rec = recordView.get(new Record(1, 1));

        // Now assume that we have some POJO classes to deserialize the binary objects.
        class JavaPerson {
            String name;
            String lastName;
        }

        class JavaPersonV2 extends JavaPerson {
            int department;
        }

        // We can have a compound record deserializing the whole tuple automatically.
        class JavaPersonRecord {
            JavaPerson originalObject;
            JavaPersonV2 upgradedObject;
            int department;
        }

        TableView<JavaPersonRecord> personRecordView = t.tableView(Mappers.ofRowClass(JavaPersonRecord.class));

        // Or we can have an arbitrary record with custom class selection.
        class TruncatedRecord {
            JavaPerson upgradedObject;
            int department;
        }

        TableView<TruncatedRecord> truncatedView = t.tableView(
            Mappers.ofRowClassBuilder(TruncatedRecord.class)
                .deserializing("upgradedObject", JavaPersonV2.class).build());

        // Or we can have a custom conditional type selection.
        TableView<TruncatedRecord> truncatedView2 = t.tableView(
            Mappers.ofRowClassBuilder(TruncatedRecord.class)
                .map("upgradedObject", (row) -> {
                    BinaryObject bobj = row.binaryObjectField("upgradedObject");
                    int dept = row.intField("department");

                    return dept == 0 ? bobj.deserialize(JavaPerson.class) : bobj.deserialize(JavaPersonV2.class);
                }).build());
    }

    /**
     * Use case 3: using simple KV mappings
     * The table has structure is
     * [
     *     [id int, orgId int] // key
     *     [name varchar, lastName varchar, decimal salary, int department] // value
     * ]
     */
    public void useCase3(Table t) {
        class EmployeeKey {
            final int id;
            final int orgId;

            EmployeeKey(int id, int orgId) {
                this.id = id;
                this.orgId = orgId;
            }
        }

        class Employee {
            String name;
            String lastName;
            BigDecimal salary;
            int department;
        }

        KVView<EmployeeKey, Employee> employeeKv = t.kvView(
            Mappers.ofKeyClass(EmployeeKey.class),
            Mappers.ofValueClass(Employee.class));

        employeeKv.get(new EmployeeKey(1, 1));

        // As described in the IEP, we can have a truncated KV mapping.
        class TruncatedEmployee {
            String name;
            String lastName;
        }

        KVView<EmployeeKey, TruncatedEmployee> truncatedEmployeeKv = t.kvView(
            Mappers.ofKeyClass(EmployeeKey.class),
            Mappers.ofValueClass(TruncatedEmployee.class));

        TruncatedEmployee te = truncatedEmployeeKv.get(new EmployeeKey(1, 1));
    }

    /**
     * Use case 4: mixing KV mapping and nested binary objects? Does this even make sense? Will keep it for symmetry.
     * The table has structure is
     * [
     *     [id int, orgId int] // key
     *     [name varchar, lastName varchar, decimal salary, int department, byte[] arbitraryNested] // value
     * ]
     */
    public void useCase4(Table t) {
        class EmployeeKey {
            final int id;
            final int orgId;

            EmployeeKey(int id, int orgId) {
                this.id = id;
                this.orgId = orgId;
            }
        }

        class EmployeeWithBinary {
            String name;
            String lastName;
            BigDecimal salary;
            int department;
            BinaryObject arbitraryNested;
        }

        KVView<EmployeeKey, EmployeeWithBinary> employeeWithBinaryKv = t.kvView(
            Mappers.ofKeyClass(EmployeeKey.class),
            Mappers.ofValueClass(EmployeeWithBinary.class));

        EmployeeWithBinary ewb = employeeWithBinaryKv.get(new EmployeeKey(1, 1));

        // Similarly to record API, we can use automatic and conditional class mapping.
        class ArbitraryObject {
            String someData;
        }

        class EmployeeDeserialized {
            String name;
            String lastName;
            BigDecimal salary;
            int department;
            ArbitraryObject arbitraryNested;
        }

        KVView<EmployeeKey, EmployeeDeserialized> employeeNestedKv = t.kvView(
            Mappers.ofKeyClass(EmployeeKey.class),
            Mappers.ofValueClass(EmployeeDeserialized.class));

        EmployeeDeserialized ewd = employeeNestedKv.get(new EmployeeKey(1, 1));

        class ArbitraryObject2 {
            int someData;
        }

        class EmployeeConditional {
            String name;
            String lastName;
            BigDecimal salary;
            int department;
            Object arbitraryNested;
        }

        KVView<EmployeeKey, EmployeeConditional> employeeCondKv = t.kvView(Mappers.ofKeyClass(EmployeeKey.class),
            Mappers.ofValueClassBuilder(EmployeeConditional.class)
                .map("arbitraryNested", (row) -> {
                    BinaryObject bobj = row.binaryObjectField("arbitraryNested");
                    int dept = row.intField("department");

                    return bobj.deserialize(dept == 0 ? ArbitraryObject.class : ArbitraryObject2.class);
                }).build());

        EmployeeConditional ec = employeeCondKv.get(new EmployeeKey(1, 1));
    }
}


