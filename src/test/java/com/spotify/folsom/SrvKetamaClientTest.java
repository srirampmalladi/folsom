package com.spotify.folsom;

import com.google.common.net.HostAndPort;
import com.spotify.dns.DnsSrvResolver;
import org.junit.Test;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import java.util.Arrays;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class SrvKetamaClientTest {
  @Test
  public void testSimple() throws Exception {
    HostAndPort hostNameA = HostAndPort.fromHost("a");
    HostAndPort hostNameB = HostAndPort.fromHost("b");
    HostAndPort hostNameC = HostAndPort.fromHost("c");
    HostAndPort hostNameD = HostAndPort.fromHost("d");

    FakeRawMemcacheClient clientA = new FakeRawMemcacheClient();
    FakeRawMemcacheClient clientB = new FakeRawMemcacheClient();
    FakeRawMemcacheClient clientC = new FakeRawMemcacheClient();
    FakeRawMemcacheClient clientD = new FakeRawMemcacheClient();

    DnsSrvResolver resolver = Mockito.mock(DnsSrvResolver.class);
    Mockito.when(resolver.resolve(Mockito.anyString()))
            .thenReturn(Arrays.asList(hostNameA, hostNameB))
            .thenReturn(Arrays.asList(hostNameC, hostNameD));

    ScheduledExecutorService executor = Mockito.mock(ScheduledExecutorService.class);
    Mockito.when(executor.scheduleAtFixedRate(
            Mockito.<Runnable>any(), Mockito.anyLong(),
            Mockito.anyLong(), Mockito.<TimeUnit>any()))
            .thenAnswer(new Answer<Object>() {
              @Override
              public Object answer(InvocationOnMock invocationOnMock) throws Throwable {
                Runnable runnable = (Runnable) invocationOnMock.getArguments()[0];

                // Run it twice to trigger two dns updates
                runnable.run();
                runnable.run();
                return Mockito.mock(ScheduledFuture.class);
              }
            });

    Mockito.when(executor.schedule(Mockito.any(Runnable.class),
            Mockito.anyLong(), Mockito.any(TimeUnit.class)))
            .thenAnswer(new Answer<Object>() {
              @Override
              public Object answer(InvocationOnMock invocationOnMock) throws Throwable {
                Runnable runnable = (Runnable) invocationOnMock.getArguments()[0];
                runnable.run();
                return null;
              }
            });


    SrvKetamaClient.Connector connector = Mockito.mock(SrvKetamaClient.Connector.class);
    Mockito.when(connector.connect(hostNameA)).thenReturn(clientA);
    Mockito.when(connector.connect(hostNameB)).thenReturn(clientB);
    Mockito.when(connector.connect(hostNameC)).thenReturn(clientC);
    Mockito.when(connector.connect(hostNameD)).thenReturn(clientD);

    SrvKetamaClient ketamaClient = new SrvKetamaClient(
            "the-srv-record", resolver, executor, 1000, TimeUnit.MILLISECONDS,
            connector, 1000, TimeUnit.MILLISECONDS);

    assertTrue(ketamaClient.isConnected());
    assertFalse(clientA.isConnected());
    assertFalse(clientB.isConnected());
    assertTrue(clientC.isConnected());
    assertTrue(clientD.isConnected());

    ketamaClient.shutdown();
    assertFalse(clientC.isConnected());
    assertFalse(clientD.isConnected());

  }
}
