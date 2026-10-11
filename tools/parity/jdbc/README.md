# IBM JCC parity worker

The standalone worker uses IBM's published `com.ibm.db2:jcc` Maven artifact, pinned to 12.1.5.0. Maven dependency resolution downloads the jar; no binary is committed. It consumes `tests/parity/cases.tsv` and emits canonical JSON Lines.

```sh
mvn -f tools/parity/jdbc/pom.xml -q package
java -cp "tools/parity/jdbc/target/classes:$HOME/.m2/repository/com/ibm/db2/jcc/12.1.5.0/jcc-12.1.5.0.jar" org.justybase.parity.Worker --all
```

Connection settings are supplied by `DB2_TEST_JDBC_URL`, `DB2_TEST_USER`, and `DB2_TEST_PASSWORD`; credentials are never included in output.
