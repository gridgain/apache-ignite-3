package org.apache.ignite.internal.schema;

import java.security.PublicKey;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import org.apache.ignite.configuration.notifications.ConfigurationNamedListListener;
import org.apache.ignite.configuration.notifications.ConfigurationNotificationEvent;
import org.apache.ignite.configuration.schemas.table.TablesConfiguration;
import org.apache.ignite.internal.causality.VersionedValue;
import org.apache.ignite.internal.configuration.schema.ExtendedTableConfiguration;
import org.apache.ignite.internal.configuration.schema.SchemaView;
import org.apache.ignite.internal.manager.IgniteComponent;
import org.apache.ignite.internal.manager.Producer;
import org.apache.ignite.internal.schema.event.SchemaEvent;
import org.apache.ignite.internal.schema.event.SchemaEventParameters;
import org.apache.ignite.internal.schema.marshaller.schema.SchemaSerializerImpl;
import org.apache.ignite.internal.schema.registry.SchemaRegistryImpl;
import org.apache.ignite.internal.util.IgniteSpinBusyLock;
import org.apache.ignite.lang.IgniteException;
import org.apache.ignite.lang.IgniteInternalException;
import org.apache.ignite.lang.IgniteLogger;
import org.apache.ignite.lang.IgniteStringFormatter;
import org.apache.ignite.lang.NodeStoppingException;

/**
 * The class services a management of table schemas.
 */
public class SchemaManager extends Producer<SchemaEvent, SchemaEventParameters> implements IgniteComponent {
    /** The logger. */
    private static final IgniteLogger LOG = IgniteLogger.forClass(SchemaManager.class);

    /** Busy lock to stop synchronously. */
    private final IgniteSpinBusyLock busyLock = new IgniteSpinBusyLock();

    /** Prevents double stopping the component. */
    private final AtomicBoolean stopGuard = new AtomicBoolean();

    /** Tables configuration. */
    private final TablesConfiguration tablesCfg;

    /** Versioned store for tables by name. */
    private final VersionedValue<Map<UUID, SchemaRegistryImpl>> registriesVv;

    /** Constructor. */
    public SchemaManager(Consumer<Consumer<Long>> registry, TablesConfiguration tablesCfg) {
        this.registriesVv = new VersionedValue<>(registry, HashMap::new);

        this.tablesCfg = tablesCfg;
    }

    /** {@inheritDoc} */
    @Override
    public void start() {
        ((ExtendedTableConfiguration) tablesCfg.tables().any()).schemas().listenElements(new ConfigurationNamedListListener<>() {
            @Override
            public CompletableFuture<?> onCreate(ConfigurationNotificationEvent<SchemaView> schemasCtx) {
                long causalityToken = schemasCtx.storageRevision();

                ExtendedTableConfiguration tblCfg = schemasCtx.config(ExtendedTableConfiguration.class);

                UUID tblId = tblCfg.id().value();

                SchemaDescriptor schemaDescriptor = SchemaSerializerImpl.INSTANCE.deserialize((schemasCtx.newValue().schema()));

                createSchema(causalityToken, tblId, schemaDescriptor);

                fireEvent(SchemaEvent.CREATE, new SchemaEventParameters(causalityToken, tblId, schemaDescriptor), null);

                return CompletableFuture.completedFuture(null);
            }
        });
    }

    private void createSchema(long causalityToken, UUID tableId, SchemaDescriptor schemaDescriptor) {
        if (!busyLock.enterBusy()) {
            throw new IgniteException(new NodeStoppingException());
        }

        try {
            createSchemaInternal(causalityToken, tableId, schemaDescriptor);
        } finally {
            busyLock.leaveBusy();
        }
    }

    private void createSchemaInternal(long causalityToken, UUID tableId, SchemaDescriptor schemaDescriptor) {
        registriesVv.update(causalityToken, registries -> {
            SchemaRegistryImpl reg = registries.get(tableId);

            if (reg == null) {
                registries = new HashMap<>(registries);

                //TODO: Closures.
                registries.put(tableId, new SchemaRegistryImpl(integer -> schemaDescriptor, () -> 1, schemaDescriptor));
            } else {
                reg.onSchemaRegistered(schemaDescriptor);
            }

            return registries;
        }, th -> {
            throw new IgniteInternalException(IgniteStringFormatter.format("Cannot create a schema for the table "
                    + "[tblId={}, ver={}]", tableId, schemaDescriptor.version()), th);
        });
    }

    public CompletableFuture<SchemaRegistry> getSchemaRegistry(long causalityToken, UUID tableId) {
        if (!busyLock.enterBusy()) {
            throw new IgniteException(new NodeStoppingException());
        }

        try {
            return registriesVv.get(causalityToken).thenApply(regs -> regs.get(tableId));
        } finally {
            busyLock.leaveBusy();
        }
    }

    public void dropRegistry(long causalityToken, UUID tableId) {
        registriesVv.update(causalityToken, registries -> {
            registries = new HashMap<>(registries);

            registries.remove(tableId);

            return registries;
        }, th -> {
            throw new IgniteInternalException(IgniteStringFormatter.format("Cannot remove a schema registry for the table "
                    + "[tblId={}]", tableId), th);
        });
    }

    /** {@inheritDoc} */
    @Override
    public void stop() throws Exception {
        if (!stopGuard.compareAndSet(false, true)) {
            return;
        }

        busyLock.block();
    }
}
