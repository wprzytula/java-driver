cd .. && mvn install -q -Dmaven.test.skip=true && cd driver-examples && mvn -q exec:java -Dexec.mainClass="com.datastax.driver.examples.opentelemetry.ZipkinUsage"
