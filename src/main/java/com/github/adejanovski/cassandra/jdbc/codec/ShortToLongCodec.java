package com.github.adejanovski.cassandra.jdbc.codec;

import com.datastax.driver.core.DataType;
import com.datastax.driver.core.ProtocolVersion;
import com.datastax.driver.core.TypeCodec;
import com.datastax.driver.core.exceptions.InvalidTypeException;
import java.nio.ByteBuffer;
import org.apache.cassandra.utils.ByteBufferUtil;

public class ShortToLongCodec extends TypeCodec<Short> {

  public ShortToLongCodec(Class<Short> javaClass) {
    super(DataType.smallint(), javaClass);
  }

  @Override
  public ByteBuffer serialize(Short paramT, ProtocolVersion paramProtocolVersion) throws InvalidTypeException {
    if (paramT == null) {
      return null;
    }
    return ByteBufferUtil.bytes(paramT.longValue());
  }

  @Override
  public Short deserialize(ByteBuffer paramByteBuffer, ProtocolVersion paramProtocolVersion) throws InvalidTypeException {
    if (paramByteBuffer == null) {
      return null;

    }
    // always duplicate the ByteBuffer instance before consuming it!
    return paramByteBuffer.duplicate().asShortBuffer().get();
  }

  @Override
  public Short parse(String paramString) throws InvalidTypeException {
    return Short.valueOf(paramString);
  }

  @Override
  public String format(Short paramT) throws InvalidTypeException {
    return String.valueOf(paramT);
  }
}
