module io.burt.athena.jdbc {
  requires java.logging;
  requires transitive java.sql;

  requires aws.xml.protocol;
  requires com.fasterxml.jackson.annotation;
  requires com.fasterxml.jackson.core;
  requires com.fasterxml.jackson.databind;
  requires slf4j.api;
  requires software.amazon.awssdk.annotations;
  requires software.amazon.awssdk.auth;
  requires software.amazon.awssdk.awscore;
  requires software.amazon.awssdk.core;
  requires software.amazon.awssdk.http;
  requires software.amazon.awssdk.profiles;
  requires software.amazon.awssdk.protocols.core;
  requires software.amazon.awssdk.protocols.json;
  requires software.amazon.awssdk.protocols.query;
  requires software.amazon.awssdk.regions;
  requires software.amazon.awssdk.utils;
  requires software.amazon.eventstream;

  requires org.reactivestreams;
  requires software.amazon.awssdk.services.athena;
  requires software.amazon.awssdk.services.s3;

  exports io.burt.athena;
  exports io.burt.athena.configuration;
  exports io.burt.athena.polling;
  exports io.burt.athena.result;
  exports io.burt.athena.result.csv;
  exports io.burt.athena.result.protobuf;
  exports io.burt.athena.result.s3;

  provides java.sql.Driver with io.burt.athena.AthenaDriver;
}
