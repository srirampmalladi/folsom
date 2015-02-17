package com.spotify.folsom;

import com.google.common.base.Function;
import com.google.common.collect.Lists;
import com.google.common.collect.Ordering;
import com.google.common.net.HostAndPort;
import com.google.common.util.concurrent.ListenableFuture;
import com.spotify.dns.DnsSrvResolver;
import com.spotify.folsom.client.NotConnectedClient;
import com.spotify.folsom.client.Request;
import com.spotify.folsom.ketama.AddressAndClient;
import com.spotify.folsom.ketama.KetamaMemcacheClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

public class SrvKetamaClient extends AbstractRawMemcacheClient {
  private static final Logger log = LoggerFactory.getLogger(SrvKetamaClient.class);

  private static final Comparator<HostAndPort> HOST_AND_PORT_COMPARATOR = new Comparator<HostAndPort>() {
    @Override
    public int compare(HostAndPort o1, HostAndPort o2) {
      int cmp = o1.getHostText().compareTo(o2.getHostText());
      if (cmp != 0) {
        return cmp;
      }
      return Integer.compare(o1.getPort(), o2.getPort());
    }
  };
  private final ScheduledExecutorService executor;
  private final long shutdownDelay;
  private final TimeUnit timeunit;

  private final ScheduledFuture<?> refreshJob;

  private final Listener connectionChangeListener;
  private final Object sync = new Object();

  private volatile List<HostAndPort> addresses = Collections.emptyList();
  private volatile RawMemcacheClient currentClient = NotConnectedClient.INSTANCE;
  private volatile RawMemcacheClient pendingClient = null;
  private boolean shutdown = false;


  public SrvKetamaClient(final String srvRecord,
                         DnsSrvResolver resolver,
                         ScheduledExecutorService executor,
                         long period, TimeUnit unit,
                         final Connector connector,
                         long shutdownDelay, TimeUnit timeunit) {
    this.shutdownDelay = shutdownDelay;
    this.timeunit = timeunit;
    this.executor = executor;
    connectionChangeListener = new Listener();
    RefreshJob scanner = new RefreshJob(resolver, srvRecord, connector);
    refreshJob = this.executor.scheduleAtFixedRate(scanner, 0, period, unit);
  }

  @Override
  public <T> ListenableFuture<T> send(Request<T> request) {
    return currentClient.send(request);
  }

  @Override
  public void shutdown() {
    refreshJob.cancel(false);
    final RawMemcacheClient pending;
    synchronized (sync) {
      shutdown = true;
      pending = pendingClient;
      pendingClient = null;
      currentClient.shutdown();
    }
    if (pending != null) {
      pending.shutdown();
    }
  }

  @Override
  public boolean isConnected() {
    return currentClient.isConnected();
  }

  @Override
  public int numTotalConnections() {
    return currentClient.numTotalConnections();
  }

  @Override
  public int numActiveConnections() {
    return currentClient.numActiveConnections();
  }

  public interface Connector {
    RawMemcacheClient connect(HostAndPort input);
  }

  private class RefreshJob implements Runnable {
    private final DnsSrvResolver srvResolver;
    private final String srvRecord;
    private final Connector connector;

    public RefreshJob(DnsSrvResolver srvResolver, String srvRecord, Connector connector) {
      this.srvResolver = srvResolver;
      this.srvRecord = srvRecord;
      this.connector = connector;
    }

    @Override
    public void run() {
      synchronized (sync) {
        if (shutdown) {
          return;
        }
        List<HostAndPort> newAddresses = Ordering.from(HOST_AND_PORT_COMPARATOR)
                .sortedCopy(srvResolver.resolve(srvRecord));
        if (!newAddresses.equals(addresses)) {
          addresses = newAddresses;
          log.info("Connecting to " + newAddresses);
          List<AddressAndClient> addressAndClients = getAddressesAndClients(newAddresses);
          setPendingClient(addressAndClients);
        }
      }
    }

    private List<AddressAndClient> getAddressesAndClients(List<HostAndPort> newAddresses) {
      return Lists.transform(newAddresses, new Function<HostAndPort, AddressAndClient>() {
        @Nullable
        @Override
        public AddressAndClient apply(@Nullable HostAndPort input) {
          return new AddressAndClient(input, connector.connect(input));
        }
      });
    }

  }

  private void setPendingClient(List<AddressAndClient> addressAndClients) {
    final RawMemcacheClient newPending;
    final RawMemcacheClient oldPending;
    synchronized (sync) {
      newPending = new KetamaMemcacheClient(addressAndClients);
      oldPending = pendingClient;
      pendingClient = newPending;
    }
    if (oldPending != null) {
      oldPending.shutdown();
    }
    newPending.registerForConnectionChanges(connectionChangeListener);

    // In case it's already connected
    connectionChangeListener.connectionChanged(newPending);
  }

  private class Listener implements ConnectionChangeListener {
    @Override
    public void connectionChanged(RawMemcacheClient client) {
      if (!client.isConnected()) {
        // If this event is not a connected-event, it is pointless to continue.
        return;
      }

      final RawMemcacheClient oldClient;
      synchronized (sync) {
        if (client != pendingClient) {
          // We don't care about this event if it's not the expected client
          return;
        }

        oldClient = currentClient;
        currentClient = pendingClient;
        pendingClient = null;
      }
      executor.schedule(new ShutdownJob(oldClient), shutdownDelay, timeunit);
    }
  }

  private class ShutdownJob implements Runnable {
    private final RawMemcacheClient oldClient;

    public ShutdownJob(RawMemcacheClient oldClient) {
      this.oldClient = oldClient;
    }

    @Override
    public void run() {
      oldClient.shutdown();
    }
  }
}
