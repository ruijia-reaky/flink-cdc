/*
 * Copyright Debezium Authors.
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle;

import io.debezium.ddl.parser.oracle.generated.PlSqlLexer;
import io.debezium.ddl.parser.oracle.generated.PlSqlParser;
import org.antlr.v4.runtime.*;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.*;

class OracleListTupleGrammarTest {
    private PlSqlParser parser(String sql) {
        PlSqlLexer lexer = new PlSqlLexer(CharStreams.fromString(sql));
        lexer.removeErrorListeners();
        PlSqlParser parser = new PlSqlParser(new CommonTokenStream(lexer));
        parser.removeErrorListeners();
        return parser;
    }

    @Test
    void acceptsTuplesAndExistingAlternatives() {
        for (String sql :
                new String[] {
                    "VALUES ((1,2),(3,4))",
                    "VALUES (1,2)",
                    "VALUES (DEFAULT)",
                    "VALUES ((TIMESTAMP '2026-01-01 00:00:00',TIMESTAMP '2026-02-01 00:00:00'))"
                }) {
            PlSqlParser p = parser(sql);
            p.list_values_clause();
            assertThat(p.getNumberOfSyntaxErrors()).as(sql).isZero();
            assertThat(p.getCurrentToken().getType()).isEqualTo(Token.EOF);
        }
    }

    @Test
    void rejectsMalformedTuple() {
        PlSqlParser p = parser("VALUES ((1,),(2,3))");
        p.list_values_clause();
        assertThat(p.getNumberOfSyntaxErrors()).isPositive();
    }

    @Test
    void preservesCompleteSingleColumnCreateTable() {
        PlSqlParser p =
                parser(
                        "CREATE TABLE T (A NUMBER) PARTITION BY LIST (A) (PARTITION P1 VALUES (1,2));");
        p.sql_script();
        assertThat(p.getNumberOfSyntaxErrors()).isZero();
    }
}
