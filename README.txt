Documentation for the Qpid components can be accessed on our website at:

http://qpid.apache.org/documentation.html

Some initial helper info can be found below.


==== Building the code and running the tests ====

Here are some example Maven build commands that you may find useful.

Clean previous builds output and install all modules to local repository without
running any of the unit or system tests.

  mvn clean install -DskipTests

Perform a subset of the tests

  mvn verify -Dtest=TestNamePattern* -DfailIfNoTests=false

Execute the unit tests and then produce the code coverage report

  mvn test jacoco:report

For more details on how to build see:
https://cwiki.apache.org/confluence/display/qpid/Qpid+Java+Build+How+To


==== Running client examples =====

Use maven to build the modules, and additionally copy the dependencies alongside their output:

  mvn clean package dependency:copy-dependencies -DskipTests

Now you can then run the examples as follows:

  java -cp "client/example/target/classes/:client/example/target/dependency/*" org.apache.qpid.example.Hello

The examples assume that a Broker is running on port 5672.
