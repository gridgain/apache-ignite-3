package org.apache.ignite.internal.sql.engine.querydb;

import java.util.List;
import java.util.stream.Collectors;
import org.apache.ignite.internal.sql.engine.prepare.ParameterMetadata;

final class DynamicParamsDetails {

    private final List<TypeDetails> types;

    DynamicParamsDetails(ParameterMetadata metadata) {
        this.types = metadata.parameterTypes().stream()
                .map(TypeDetails::getParamType)
                .collect(Collectors.toList());
    }

    List<TypeDetails> types() {
        return types;
    }
}
