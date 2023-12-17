package ch.cypherchecker.common;

import ch.cypherchecker.common.schema.Schema;

public interface OracleFactory<C extends Connection, T> {

    Oracle createOracle(OracleType type, GlobalState<C> state, Schema<T> schema);

}
