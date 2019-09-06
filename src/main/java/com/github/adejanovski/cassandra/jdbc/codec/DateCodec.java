package com.github.adejanovski.cassandra.jdbc.codec;

import com.datastax.driver.core.DataType;
import com.datastax.driver.core.LocalDate;
import com.datastax.driver.core.ProtocolVersion;
import com.datastax.driver.core.TypeCodec;
import com.datastax.driver.core.exceptions.InvalidTypeException;
import java.nio.ByteBuffer;
import java.util.Date;
import org.apache.cassandra.utils.ByteBufferUtil;

public class DateCodec extends TypeCodec<Date> {

  private final TypeCodec<LocalDate> innerCodec;

  public DateCodec(TypeCodec<LocalDate> codec, Class<Date> javaClass) {
    super(codec.getCqlType(), javaClass);
    innerCodec = codec;
  }

  @Override
  public ByteBuffer serialize(Date value, ProtocolVersion protocolVersion) throws InvalidTypeException {
    return innerCodec.serialize(LocalDate.fromMillisSinceEpoch(value.getTime()), protocolVersion);
  }

  @Override
  public Date deserialize(ByteBuffer bytes, ProtocolVersion protocolVersion) throws InvalidTypeException {
    return new Date(innerCodec.deserialize(bytes, protocolVersion).getMillisSinceEpoch());
  }

  @Override
  public Date parse(String value) throws InvalidTypeException {
    return new Date(innerCodec.parse(value).getMillisSinceEpoch());
  }

  @Override
  public String format(Date value) throws InvalidTypeException {
    return value.toString();
  }

}
