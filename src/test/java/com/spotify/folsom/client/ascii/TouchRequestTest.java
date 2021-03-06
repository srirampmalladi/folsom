/*
 * Copyright (c) 2015 Spotify AB
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

package com.spotify.folsom.client.ascii;

import com.spotify.folsom.MemcacheStatus;
import org.junit.Test;

import java.io.IOException;
import java.util.concurrent.ExecutionException;

import static org.junit.Assert.assertEquals;


public class TouchRequestTest extends RequestTestTemplate {

  private TouchRequest req = new TouchRequest("foo", 123);

  @Test
  public void testRequest() throws Exception {
    assertRequest(req, "touch foo 123\r\n");
  }

  @Test
  public void testResponse() throws IOException, InterruptedException, ExecutionException {
    req.handle(AsciiResponse.TOUCHED);

    assertEquals(MemcacheStatus.OK, req.get());
  }

  @Test
  public void testNonFoundResponse() throws IOException, InterruptedException, ExecutionException {
    req.handle(AsciiResponse.NOT_FOUND);
    assertEquals(MemcacheStatus.KEY_NOT_FOUND, req.get());
  }
}
