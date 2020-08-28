package com.github.adejanovski.cassandra.jdbc.codec;

import static com.datastax.driver.core.CodecUtils.fromCqlDateToDaysSinceEpoch;
import static com.datastax.driver.core.CodecUtils.fromSignedToUnsignedInt;
import static com.datastax.driver.core.CodecUtils.fromUnsignedToSignedInt;
import static com.datastax.driver.core.ParseUtils.isLongLiteral;
import static com.datastax.driver.core.ParseUtils.isQuoted;
import static com.datastax.driver.core.ParseUtils.unquote;
import static java.lang.Long.parseLong;

import com.datastax.driver.core.DataType;
import com.datastax.driver.core.ProtocolVersion;
import com.datastax.driver.core.TypeCodec;
import com.datastax.driver.core.exceptions.InvalidTypeException;
import java.nio.ByteBuffer;
import java.time.LocalDate;

public class DateToStringCodec extends TypeCodec<String> {

  private static final java.time.LocalDate EPOCH = java.time.LocalDate.of(1970, 1, 1);

  public DateToStringCodec(Class<String> javaClass) {
    super(DataType.date(), javaClass);
  }

  @Override
  public ByteBuffer serialize(String value, ProtocolVersion protocolVersion) throws InvalidTypeException {
    if (value == null) {
      return null;
    }
    long days = java.time.temporal.ChronoUnit.DAYS.between(EPOCH, LocalDate.parse(value));
    int unsigned = fromSignedToUnsignedInt((int) days);
    return cint().serializeNoBoxing(unsigned, protocolVersion);
  }

  @Override
  public String deserialize(ByteBuffer bytes, ProtocolVersion protocolVersion) throws InvalidTypeException {
    if (bytes == null || bytes.remaining() == 0)
      return null;
    int unsigned = cint().deserializeNoBoxing(bytes, protocolVersion);
    int signed = fromUnsignedToSignedInt(unsigned);
    return EPOCH.plusDays(signed).toString();
  }

  @Override
  public String parse(String value) throws InvalidTypeException {
    if (value == null || value.isEmpty() || value.equalsIgnoreCase("NULL"))
      return null;

    // single quotes are optional for long literals, mandatory for date patterns
    // strip enclosing single quotes, if any
    if (isQuoted(value))
      value = unquote(value);

    if (isLongLiteral(value)) {
      long raw;
      try {
        raw = parseLong(value);
      } catch (NumberFormatException e) {
        throw new InvalidTypeException(String.format("Cannot parse date value from \"%s\"", value));
      }
      int days;
      try {
        days = fromCqlDateToDaysSinceEpoch(raw);
      } catch (IllegalArgumentException e) {
        throw new InvalidTypeException(String.format("Cannot parse date value from \"%s\"", value));
      }
      return EPOCH.plusDays(days).toString();
    }

    try {
      return java.time.LocalDate.parse(value, java.time.format.DateTimeFormatter.ISO_LOCAL_DATE).toString();
    } catch (RuntimeException e) {
      throw new InvalidTypeException(String.format("Cannot parse date value from \"%s\"", value));
    }
  }

  @Override
  public String format(String value) throws InvalidTypeException {
    return value;
  }

}
