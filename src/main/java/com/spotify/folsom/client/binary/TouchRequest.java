/*
 * Copyright (c) 2014-2015 Spotify AB
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */


package com.spotify.folsom.client.binary;

import com.spotify.folsom.MemcacheStatus;
import com.spotify.folsom.client.OpCode;
import com.spotify.folsom.client.Utils;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;

import java.io.IOException;
import java.nio.ByteBuffer;

public class TouchRequest extends BinaryRequest<MemcacheStatus> {

  private final int ttl;

  public TouchRequest(final String key,
                      final int ttl,
                      final int opaque) {
    super(key, opaque);
    this.ttl = ttl;
  }

  @Override
  public ByteBuf writeRequest(final ByteBufAllocator alloc, final ByteBuffer dst) {
    final int keyLength = key.length();

    final int expiration = Utils.ttlToExpiration(ttl);
    final int extrasLength = 4;
    final int totalLength = keyLength + extrasLength;

    writeBinaryHeader(dst, OpCode.TOUCH, keyLength, extrasLength, totalLength, 0, opaque);
    dst.putInt(expiration);
    Utils.writeKeyString(dst, key);

    return toBuffer(alloc, dst);
  }

  @Override
  public void handle(final BinaryResponse replies) throws IOException {
    ResponsePacket reply = handleSingleReply(replies);
    succeed(reply.status);
  }
}