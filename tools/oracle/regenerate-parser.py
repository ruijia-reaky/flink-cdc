#!/usr/bin/env python3
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements. See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License. You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Regenerate the Oracle parser using a supplied ANTLR 4.10.1 complete JAR."""
import argparse
import hashlib
from pathlib import Path
import subprocess
import tempfile

parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument("--antlr-jar", type=Path, required=True)
parser.add_argument("--java", default="java")
args = parser.parse_args()
jar = args.antlr_jar.resolve()
expected = "41949d41f20d31d5b8277187735dd755108df52b38db6c865108d3382040f918"
if hashlib.sha256(jar.read_bytes()).hexdigest() != expected:
    raise SystemExit("Expected the official antlr-4.10.1-complete.jar (SHA-256 mismatch)")

module = (Path(__file__).resolve().parents[2]
          / "flink-cdc-connect/flink-cdc-source-connectors/flink-connector-oracle-cdc")
package = Path("io/debezium/ddl/parser/oracle/generated")
grammar = module / "src/main/antlr4" / package
destination = module / "src/main/java" / package
with tempfile.TemporaryDirectory(prefix="oracle-antlr-") as generated:
    subprocess.run([args.java, "-jar", str(jar), "-package",
                    "io.debezium.ddl.parser.oracle.generated", "-o", generated,
                    "PlSqlLexer.g4", "PlSqlParser.g4"], cwd=grammar, check=True)
    # Lexer grammar is unchanged; keep using the upstream lexer from the dependency.
    for name in ("PlSqlParser.java", "PlSqlParserListener.java", "PlSqlParserBaseListener.java"):
        content = (Path(generated) / name).read_text(encoding="utf-8")
        (destination / name).write_text(content, encoding="utf-8", newline="\n")
print("Regenerated parser and listeners; lexer dependency unchanged.")
