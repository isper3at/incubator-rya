/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.rya.mongodb.document.visibility;

import static com.google.common.base.Charsets.UTF_8;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import org.apache.hadoop.io.Text;

public class ByteBufferUtil {
  public static byte[] toBytes(final ByteBuffer buffer) {
    if (buffer == null) {
		return null;
	}
    return Arrays.copyOfRange(buffer.array(), buffer.position(), buffer.limit());
  }

  public static List<ByteBuffer> toByteBuffers(final Collection<byte[]> bytesList) {
    if (bytesList == null) {
		return null;
	}
    final ArrayList<ByteBuffer> result = new ArrayList<ByteBuffer>();
    for (final byte[] bytes : bytesList) {
      result.add(ByteBuffer.wrap(bytes));
    }
    return result;
  }

  public static List<byte[]> toBytesList(final Collection<ByteBuffer> bytesList) {
    if (bytesList == null) {
		return null;
	}
    final ArrayList<byte[]> result = new ArrayList<byte[]>();
    for (final ByteBuffer bytes : bytesList) {
      result.add(toBytes(bytes));
    }
    return result;
  }

  public static Text toText(final ByteBuffer bytes) {
    if (bytes == null) {
		return null;
	}
    final Text result = new Text();
    result.set(bytes.array(), bytes.position(), bytes.remaining());
    return result;
  }

  public static String toString(final ByteBuffer bytes) {
    return new String(bytes.array(), bytes.position(), bytes.remaining(), UTF_8);
  }

  public static ByteBuffer toByteBuffers(final ByteSequence bs) {
    if (bs == null) {
		return null;
	}

    if (bs.isBackedByArray()) {
      return ByteBuffer.wrap(bs.getBackingArray(), bs.offset(), bs.length());
    } else {
      // TODO create more efficient impl
      return ByteBuffer.wrap(bs.toArray());
    }
  }
}
