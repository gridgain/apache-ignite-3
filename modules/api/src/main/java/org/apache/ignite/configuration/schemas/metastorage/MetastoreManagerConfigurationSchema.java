package org.apache.ignite.configuration.schemas.metastorage;

import org.apache.ignite.configuration.annotation.ConfigurationRoot;
import org.apache.ignite.configuration.annotation.Value;
import org.apache.ignite.configuration.schemas.TempConfigurationStorage;

@ConfigurationRoot(rootName = "metastore", storage = TempConfigurationStorage.class)
public class MetastoreManagerConfigurationSchema {
    @Value
    String[] names;
}
