package com.github.adejanovski.cassandra.jdbc.codec;

import com.datastax.driver.core.DataType;
import com.datastax.driver.core.ParseUtils;
import com.datastax.driver.core.ProtocolVersion;
import com.datastax.driver.core.TypeCodec;
import com.datastax.driver.core.exceptions.InvalidTypeException;
import java.nio.ByteBuffer;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;

public class TimeToStringCodec extends TypeCodec<String> {

  private static final java.time.format.DateTimeFormatter FORMATTER = DateTimeFormatter.ISO_LOCAL_TIME;

  public TimeToStringCodec(Class<String> javaClass) {
    super(DataType.time(), javaClass);
  }

  @Override
  public ByteBuffer serialize(String value, ProtocolVersion protocolVersion) throws InvalidTypeException {
    if (value == null) {
      return null;
    }
    return bigint().serializeNoBoxing(LocalTime.parse(value).toNanoOfDay(), protocolVersion);
  }

  @Override
  public String deserialize(ByteBuffer bytes, ProtocolVersion protocolVersion) throws InvalidTypeException {
    if (bytes == null || bytes.remaining() == 0)
      return null;
    long nanosOfDay = bigint().deserializeNoBoxing(bytes, protocolVersion);
    return java.time.LocalTime.ofNanoOfDay(nanosOfDay).format(FORMATTER);
  }

  @Override
  public String parse(String value) throws InvalidTypeException {
    if (value == null || value.isEmpty() || value.equalsIgnoreCase("NULL"))
      return null;

    // enclosing single quotes required, even for long literals
    if (!ParseUtils.isQuoted(value)) {
      throw new InvalidTypeException("time values must be enclosed by single quotes");
    }

    value = value.substring(1, value.length() - 1);

    if (ParseUtils.isLongLiteral(value)) {
      long nanosOfDay;
      try {
        nanosOfDay = Long.parseLong(value);
      } catch (NumberFormatException e) {
        throw new InvalidTypeException(String.format("Cannot parse time value from \"%s\"", value), e);
      }

      return java.time.LocalTime.ofNanoOfDay(nanosOfDay).format(FORMATTER);
    }

    try {
      return java.time.LocalTime.parse(value).format(FORMATTER);
    } catch (RuntimeException e) {
      throw new InvalidTypeException(String.format("Cannot parse time value from \"%s\"", value), e);
    }
  }

  @Override
  public String format(String value) throws InvalidTypeException {
    return value;
  }

}
