# athena-jdbc

[![Build Status](https://travis-ci.org/burtcorp/athena-jdbc.png?branch=master)](https://travis-ci.org/burtcorp/athena-jdbc) [![Maven Central](https://maven-badges.herokuapp.com/maven-central/io.burt/athena-jdbc/badge.svg)](https://maven-badges.herokuapp.com/maven-central/io.burt/athena-jdbc)


_If you're reading this on GitHub, please note that this is the readme for the development version and that some features described here might not yet have been released. You can find the readme for a specific version via the release tags ([here is an example](https://github.com/burtcorp/athena-jdbc/releases/tag/athena-jdbc-0.1.0))._

This is a JDBC driver for AWS Athena.

## Installation

Using Maven you can add this to your dependencies:

```xml
<dependency>
  <groupId>io.burt</groupId>
  <artifactId>athena-jdbc</artifactId>
  <version>${athena-jdbc.version}</version>
</dependency>
```

Check the [releases page](https://github.com/burtcorp/athena-jdbc/releases) for the value of `${athena-jdbc.version}`.

### Dependencies

The driver requires Java 8 or later.

The only dependency is [AWS SDK](https://github.com/aws/aws-sdk-java-v2), specifically `software.amazon.awssdk:athena`. See `pom.xml` for more details.

## Usage

### JDBC URLs

The driver registers itself with `java.sql.DriverManager` automatically, and accepts JDBC URLs with the subprotocol `athena`. The subname is the default database name for the connection, and is optional. The simplest possible JDBC URL is `"jdbc:athena"`, which is equivalent to `"jdbc:athena:default"`. To use a database called "test" as the default database for the connection use the URL `"jdbc:athena:test"`.

### Driver and data source classes

When using a JDBC URL to connect you shouldn't need the driver class name, but if you for some reason need them, driver class name is `io.burt.athena.AthenaDriver` and the data source class name is `io.burt.athena.AthenaDataSource`.

### Connection properties

There are three connection properties:

* `region`: the AWS region to connecto to. The AWS SDK will automatically pick up the value of the `AWS_REGION` environment variable if it is set.
* `outputLocation`: the location in Amazon S3 where the query results will be stored. This property is required unless `workGroup` is set to a work group that has a configured output location. See [the API docs for more information](https://docs.aws.amazon.com/athena/latest/APIReference/API_ResultConfiguration.html#athena-Type-ResultConfiguration-OutputLocation).
* `workGroup`: the name of the work group in which to run the query. See [the API docs for more information](https://docs.aws.amazon.com/athena/latest/APIReference/API_StartQueryExecution.html#athena-StartQueryExecution-request-WorkGroup).

These properties are the same for both the `java.sql.DriverManager` and `javax.sql.DataSource` APIs.

### Examples

#### Connecting with `DriverManager`

```java
import java.sql.Connection;
import java.sql.DriverManager;
import java.util.Properties;

Properties properties = new Properties();
properties.setProperty("region", "us-east-1"); // or set AWS_REGION
properties.setProperty("outputLocation", "s3://some-bucket/and/a/prefix");

try (Connection connection = DriverManager.getConnection("jdbc:athena:default", properties)) {
  // use connection
}
```

Currently the `java.sql.Connection` returned by the driver is thread safe and doesn't need to be closed until the application stops, if at all.

#### Creating a `DataSource`

This is the least amount of code needed to obtain a connection, and you also get type safe configuration:

```java
import io.burt.ahtena.AthenaDataSource;
import java.sql.Connection;
import javax.sql.DataSource;

AthenaDataSource dataSource = new AthenaDataSource();
dataSource.setRegion("us-east-1");
dataSource.setOutputLocation("s3://some-bucket/and/a/prefix")

try (Connection connection = dataSource.getConnection()) {
}
```

#### Connecting with a connection pool

This example uses [HikariCP](https://github.com/brettwooldridge/HikariCP), but of course other connection pools work too. The driver supports both JDBC URL and `javax.sql.DataSource` based configurations, and both use the same property names.

```java
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import java.sql.Connection;
import javax.sql.DataSource;

HikariConfig config = new HikariConfig();
config.setJdbcUrl("jdbc:athena:default"); // or setDataSourceClassName("io.burt.athena.AthenaDataSource")
config.addDataSourceProperty("region", "us-east-1"); // or set AWS_REGION
config.addDataSourceProperty("outputLocation", "s3://some-bucket/and/a/prefix");
DataSource dataSource = new HikariDataSource(config);

try (Connection connection = dataSource.getConnection()) {
  // use connection
}
```

#### Using a connection

Once you have a `java.sql.Connection` instance you can use it as you would one from any other JDBC driver.

```java
import java.sql.ResultSet;
import java.sql.Statement;

try (
  Statement statement = connection.createStatement();
  ResultSet resultSet = statement.executeQuery("SELECT 'Hello from Athena'")
) {
  resultSet.next();
  System.out.println(resultSet.getString(1));
}
```

## Description

### Why another Athena JDBC driver?

There is already an [existing JDBC driver for Athena](https://docs.aws.amazon.com/athena/latest/ug/connect-with-jdbc.html), so why another one?

There is nothing fundamentally wrong with the existing driver. It was developed by Simba, who have a lot of experience building JDBC drivers, but who does not have much experience with Athena. Just providing a JDBC layer for running queries in Athena is not enough.

* There is no way to get hold of the query execution ID of a query. This means that you can't get query statistics, or log the query ID for debugging, and many other things.
* There is no support for things like client request tokens, which means that probably the best way to achieve good performance and avoid costs in Athena is not possible.
* The polling strategy is poor. Initially it was more or less calling `GetQueryExecution` in a loop with a 5ms delay, which caused a lot of throttling errors if you had a few applications using the driver. In later versions it uses a backoff strategy of sorts; it starts at 5ms and increases the delay to a configurable max value. The increase is by a factor of 20, though, which means that with the defaults it will start at 5ms and then go to 100ms, the default max, which is more or less equivalent to a fixed delay of 100ms. If you give it a higher max, say 2000ms, that will be reached after two calls.
* It shades the Athena and Glue AWS SDKs, which, among other things, causes issues with logging (see below) and with exceptions. A JDBC driver throws `SQLException`, of course, but the original exceptions are usually available as the exception cause. However, since the AWS SDK is shaded these exceptions have internal class names instead of the well-known AWS SDK names, making it hard to deal with unwrapping exceptions and deal with the underlying cause.
* The driver uses its own home-grown logging framework, which does not interact with any common Java logging framework. It's hard to configure, and most things you can't even configure. You'd be forgiven for thinking that properties like `UseAwsLogger` meant that it would use the AWS SDK logging facilities (which use SLF4J, as you'd expect), but instead it means that it will log all the AWS SDK calls it makes to its own logger. Also, to add insult to injury, because it shades the AWS SDK configuring logging for that becomes much harder. The driver does not document how the AWS SDK has been shaded, so you'll have to figure out yourself what the class names are. For some reason it also shades log4j, making the situation even more of a mess, and since it's shaded, methods like using the SLF4J briges don't work.
* The driver is also not available in any Maven repository, which means that any project depending on it will have to find its own way to get hold of the JAR.
* Finally, most of the shortcomings above could have been fixed easily if the driver was open source. We would have loved to provide patches that fixed the issues we have found over the last few years, instead of having to write our own.

This alternative Athena JDBC driver aims to fix these issues, and hopefully even more things that we haven't even thought of yet.

## Limitations & known issues

If you get a `java.lang.UnsupportedOperationException` it means that you've found a feature that hasn't been implemented yet. Some of these will eventually get implemented, but some may not make sense for Athena. If you rely on one of these open an issue and describe your use case.

If you get a `java.sql.SQLFeatureNotSupportedException` exception it means that the feature or operation either is not supported by Athena, or that it's unclear how it could be supported. Feel free to open pull requests that adds support for it if you have an idea for how it could be done.

Before you open an issue, check the API documentation and tests, there might be clues and hints there. Looking at the commit history of a method can also lead to insights.

These are some specific limitations and known issues that you might run into:

* `ResultSet#getArray` always returns string arrays, because Athena does not return any type information beyond `"array"`. It also does it's best splitting the array, but there is no way to tell the arrays `["hello", "world"]` and `["hello, world"]` apart. We recommend always casting to JSON for complex types, extract them using `ResultSet#getString` and parse them in your own code.
* Similarily to arrays, maps and structs don't have unambiguous serializations in the Athena output format, but there is also no support in the JDBC API for these types. Cast to JSON, and use `ResultSet#getString` and parse them in your own code.
* `Connection#prepareStatement` is not supported. The official Athena driver tries to support prepared statements and interpolation on the client side (it's unclear if it even works), but it's not the goal of this alternative driver to do that. Athena itself does not support prepared statements or interpolation, and there is no performance gain to be had from preparing statements.

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md).

## Copyright

© 2019 Theo Hultberg and contributors, see LICENSE.txt (BSD 3-Clause).
