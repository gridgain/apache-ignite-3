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

namespace Apache.Ignite.Internal.Table
{
    using System;
    using System.Collections.Concurrent;
    using System.Collections.Generic;
    using System.Diagnostics;
    using System.Threading;
    using System.Threading.Tasks;
    using Buffers;
    using Common;
    using Ignite.Sql;
    using Ignite.Table;
    using Ignite.Transactions;
    using Microsoft.Extensions.Logging;
    using Proto;
    using Proto.MsgPack;
    using Serialization;
    using Sql;

    /// <summary>
    /// Table API.
    /// </summary>
    internal sealed class Table : ITable
    {
        /// <summary>
        /// Unknown schema version.
        /// </summary>
        public const int SchemaVersionUnknown = -1;

        /// <summary>
        /// Latest schema version, bypassing cache.
        /// </summary>
        public const int SchemaVersionForceLatest = -2;

        /** Socket. */
        private readonly ClientFailoverSocket _socket;

        /** SQL. */
        private readonly Sql _sql;

        /** Schemas. */
        private readonly ConcurrentDictionary<int, Task<Schema>> _schemas = new();

        /** Cached record views. */
        private readonly ConcurrentDictionary<Type, object> _recordViews = new();

        /** */
        private readonly object _latestSchemaLock = new();

        /** */
        private readonly ILogger _logger;

        /** */
        private readonly SemaphoreSlim _partitionAssignmentSemaphore = new(1);

        /** */
        private volatile int _latestSchemaVersion = SchemaVersionUnknown;

        /** */
        private volatile int _partitionAssignmentVersion = -1;

        /** */
        private volatile string?[]? _partitionAssignment;

        /// <summary>
        /// Initializes a new instance of the <see cref="Table"/> class.
        /// </summary>
        /// <param name="name">Table name.</param>
        /// <param name="id">Table id.</param>
        /// <param name="socket">Socket.</param>
        /// <param name="sql">SQL.</param>
        public Table(string name, int id, ClientFailoverSocket socket, Sql sql)
        {
            _socket = socket;
            _sql = sql;

            Name = name;
            Id = id;

            _logger = socket.Configuration.LoggerFactory.CreateLogger<Table>();

            RecordBinaryView = new RecordView<IIgniteTuple>(
                this,
                new RecordSerializer<IIgniteTuple>(this, TupleSerializerHandler.Instance),
                _sql);

            // RecordView and KeyValueView are symmetric and perform the same operations on the protocol level.
            // Only serialization is different - KeyValueView splits records into two parts.
            // Therefore, KeyValueView below simply delegates to RecordView<KvPair>,
            // and SerializerHandler writes KV pair as a single record and reads back record as two parts.
            var pairSerializer = new RecordSerializer<KvPair<IIgniteTuple, IIgniteTuple>>(this, TuplePairSerializerHandler.Instance);

            KeyValueBinaryView = new KeyValueView<IIgniteTuple, IIgniteTuple>(
                new RecordView<KvPair<IIgniteTuple, IIgniteTuple>>(this, pairSerializer, _sql));
        }

        /// <inheritdoc/>
        public string Name { get; }

        /// <inheritdoc/>
        public IRecordView<IIgniteTuple> RecordBinaryView { get; }

        /// <inheritdoc/>
        public IKeyValueView<IIgniteTuple, IIgniteTuple> KeyValueBinaryView { get; }

        /// <summary>
        /// Gets the associated socket.
        /// </summary>
        internal ClientFailoverSocket Socket => _socket;

        /// <summary>
        /// Gets the table id.
        /// </summary>
        internal int Id { get; }

        /// <inheritdoc/>
        public IRecordView<T> GetRecordView<T>()
            where T : notnull => GetRecordViewInternal<T>();

        /// <inheritdoc/>
        public IKeyValueView<TK, TV> GetKeyValueView<TK, TV>()
            where TK : notnull
            where TV : notnull =>
            new KeyValueView<TK, TV>(GetRecordViewInternal<KvPair<TK, TV>>());

        /// <inheritdoc/>
        public override string ToString() =>
            new IgniteToStringBuilder(GetType())
                .Append(Name)
                .Append(Id)
                .Build();

        /// <summary>
        /// Gets the record view for the specified type.
        /// </summary>
        /// <typeparam name="T">Record type.</typeparam>
        /// <returns>Record view.</returns>
        internal RecordView<T> GetRecordViewInternal<T>()
            where T : notnull
        {
            // ReSharper disable once HeapView.CanAvoidClosure (generics prevent this)
            return (RecordView<T>)_recordViews.GetOrAdd(
                typeof(T),
                _ => new RecordView<T>(this, new RecordSerializer<T>(this, new ObjectSerializerHandler<T>()), _sql));
        }

        /// <summary>
        /// Reads the schema.
        /// </summary>
        /// <param name="buf">Buffer.</param>
        /// <returns>Schema or null.</returns>
        internal Task<Schema> ReadSchemaAsync(PooledBuffer buf)
        {
            var version = buf.GetReader().ReadInt32();

            return GetCachedSchemaAsync(version);
        }

        /// <summary>
        /// Gets the schema by version.
        /// </summary>
        /// <param name="version">Schema version; when null, latest is used.</param>
        /// <returns>Schema.</returns>
        internal Task<Schema> GetSchemaAsync(int? version) => version == SchemaVersionForceLatest
            ? LoadSchemaAsync(SchemaVersionUnknown)
            : GetCachedSchemaAsync(version ?? _latestSchemaVersion);

        /// <summary>
        /// Gets the preferred node by colocation hash.
        /// </summary>
        /// <param name="colocationHash">Colocation hash.</param>
        /// <param name="transaction">Transaction.</param>
        /// <returns>Preferred node.</returns>
        internal async ValueTask<PreferredNode> GetPreferredNode(int colocationHash, ITransaction? transaction)
        {
            if (transaction != null)
            {
                return default;
            }

            var assignment = await GetPartitionAssignmentAsync().ConfigureAwait(false);

            if (assignment == null || assignment.Length == 0)
            {
                // Happens on table drop.
                return default;
            }

            var partition = Math.Abs(colocationHash % assignment.Length);
            var nodeConsistentId = assignment[partition];

            return PreferredNode.FromName(nodeConsistentId);
        }

        /// <summary>
        /// Gets the partition assignment.
        /// </summary>
        /// <returns>Partition assignment.</returns>
        internal async ValueTask<string?[]?> GetPartitionAssignmentAsync()
        {
            var socketVer = _socket.PartitionAssignmentVersion;
            var assignment = _partitionAssignment;

            // Async double-checked locking. Assignment changes rarely, so we avoid the lock if possible.
            if (_partitionAssignmentVersion == socketVer && assignment != null)
            {
                return assignment;
            }

            await _partitionAssignmentSemaphore.WaitAsync().ConfigureAwait(false);

            try
            {
                socketVer = _socket.PartitionAssignmentVersion;
                assignment = _partitionAssignment;

                if (_partitionAssignmentVersion == socketVer && assignment != null)
                {
                    return assignment;
                }

                assignment = await LoadPartitionAssignmentAsync().ConfigureAwait(false);

                _partitionAssignment = assignment;
                _partitionAssignmentVersion = socketVer;

                return assignment;
            }
            finally
            {
                _partitionAssignmentSemaphore.Release();
            }
        }

        private Task<Schema> GetCachedSchemaAsync(int version)
        {
            var task = GetOrAdd();

            if (!task.IsFaulted)
            {
                return task;
            }

            // Do not return failed task. Remove it from the cache and try again.
            _schemas.TryRemove(new KeyValuePair<int, Task<Schema>>(version, task));

            return GetOrAdd();

            Task<Schema> GetOrAdd() => _schemas.GetOrAdd(version, static (ver, tbl) => tbl.LoadSchemaAsync(ver), this);
        }

        /// <summary>
        /// Loads the schema.
        /// </summary>
        /// <param name="version">Version.</param>
        /// <returns>Schema.</returns>
        private async Task<Schema> LoadSchemaAsync(int version)
        {
            using var writer = ProtoCommon.GetMessageWriter();
            Write();

            using var resBuf = await _socket.DoOutInOpAsync(ClientOp.SchemasGet, writer).ConfigureAwait(false);
            return Read();

            void Write()
            {
                var w = writer.MessageWriter;
                w.Write(Id);

                if (version == SchemaVersionUnknown)
                {
                    w.WriteNil();
                }
                else
                {
                    w.Write(1);
                    w.Write(version);
                }
            }

            Schema Read()
            {
                var r = resBuf.GetReader();
                var schemaCount = r.ReadInt32();

                if (schemaCount == 0)
                {
                    throw new IgniteClientException(ErrorGroups.Client.Protocol, "Schema not found: " + version);
                }

                Schema last = null!;

                for (var i = 0; i < schemaCount; i++)
                {
                    last = ReadSchema(ref r);
                }

                // Store all schemas in the map, and return last.
                return last;
            }
        }

        /// <summary>
        /// Reads the schema.
        /// </summary>
        /// <param name="r">Reader.</param>
        /// <returns>Schema.</returns>
        private Schema ReadSchema(ref MsgPackReader r)
        {
            var schemaVersion = r.ReadInt32();
            var columnCount = r.ReadInt32();
            var keyColumnCount = 0;
            var colocationColumnCount = 0;

            var columns = new Column[columnCount];

            for (var i = 0; i < columnCount; i++)
            {
                var propertyCount = r.ReadInt32();
                const int expectedCount = 7;

                Debug.Assert(propertyCount >= expectedCount, "propertyCount >= " + expectedCount);

                var name = r.ReadString();
                var type = r.ReadInt32();
                var isKey = r.ReadBoolean();
                var isNullable = r.ReadBoolean();
                var colocationIndex = r.ReadInt32();
                var scale = r.ReadInt32();
                var precision = r.ReadInt32();

                r.Skip(propertyCount - expectedCount);

                var column = new Column(name, (ColumnType)type, isNullable, isKey, colocationIndex, i, scale, precision);

                columns[i] = column;

                if (isKey)
                {
                    keyColumnCount++;
                }

                if (colocationIndex >= 0)
                {
                    colocationColumnCount++;
                }
            }

            var schema = new Schema(
                Version: schemaVersion,
                TableId: Id,
                KeyColumnCount: keyColumnCount,
                ColocationColumnCount: colocationColumnCount,
                Columns: columns);

            _schemas[schemaVersion] = Task.FromResult(schema);

            _logger.LogSchemaLoadedDebug(Id, schema.Version);

            lock (_latestSchemaLock)
            {
                if (schemaVersion > _latestSchemaVersion)
                {
                    _latestSchemaVersion = schemaVersion;
                }
            }

            return schema;
        }

        /// <summary>
        /// Loads the partition assignment.
        /// </summary>
        /// <returns>Partition assignment.</returns>
        private async Task<string?[]?> LoadPartitionAssignmentAsync()
        {
            using var writer = ProtoCommon.GetMessageWriter();
            Write(writer.MessageWriter);

            using var resBuf = await _socket.DoOutInOpAsync(ClientOp.PartitionAssignmentGet, writer).ConfigureAwait(false);
            return Read();

            void Write(MsgPackWriter w)
            {
                w.Write(Id);
                w.Write(0); // TODO IGNITE-20900: Send timestamp.
            }

            string?[]? Read()
            {
                var r = resBuf.GetReader();
                var count = r.ReadInt32();

                if (count == 0)
                {
                    return null;
                }

                // TODO IGNITE-20900: Handle timestamp.
                _ = r.ReadInt64();

                var res = new string?[count];

                for (int i = 0; i < count; i++)
                {
                    res[i] = r.ReadStringNullable();
                }

                return res;
            }
        }
    }
}
