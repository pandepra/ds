package com.pandepra.ds;

import java.util.HashSet;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.DelayQueue;
import java.util.concurrent.Delayed;
import java.util.concurrent.TimeUnit;

/**
 * A HashSet with expirable keys. Not thread-safe. This implementation uses a {@link DelayQueue} to
 * remove expired keys which is much more efficient than scanning the entire map.
 *
 * @param <E> the type param
 */
public class ExpirableHashSet<E> extends HashSet<E> {

  private long globalDelay;
  private TimeUnit globalDelayTimeUnit;

  private final BlockingQueue<Expirable> delayedQueue = new DelayQueue<>();

  public ExpirableHashSet(long globalDelay, TimeUnit globalDelayTimeUnit) {
    super();
    this.globalDelay = globalDelay;
    this.globalDelayTimeUnit = globalDelayTimeUnit;
  }

  public boolean add(E element) {
    cleanup();
    return super.add(element);
  }

  public boolean add(E element, long ttl, TimeUnit timeUnit) {
    cleanup();
    Expirable elem = new Expirable(element, ttl, timeUnit);
    delayedQueue.add(elem);
    return add(element);
  }

  /*Removes all expired keys from the set*/
  private void cleanup() {
    while (null != delayedQueue.peek()) {
      Expirable poll = delayedQueue.poll();
      super.remove(poll.value());
    }
  }

  /** An element that may expire at some point in future. Supports millis resolution */
  private class Expirable implements Delayed {

    private final long expireAt;
    private final E value;

    public Expirable(E value, long ttl, TimeUnit unit) {
      this.expireAt = ttl < 0 ? Long.MAX_VALUE : System.currentTimeMillis() + unit.toMillis(ttl);
      this.value = value;
    }

    public E value() {
      return this.value;
    }

    public long expireAt() {
      return this.expireAt;
    }

    @Override
    public int hashCode() {
      return value.hashCode();
    }

    @Override
    public boolean equals(Object o) {
      if (!(o instanceof ExpirableHashSet.Expirable)) {
        return false;
      }
      Expirable o1 = (Expirable) o;
      return o1.value().equals(value);
    }

    @Override
    public long getDelay(TimeUnit unit) {
      return unit.convert(expireAt - System.currentTimeMillis(), TimeUnit.MILLISECONDS);
    }

    @Override
    public int compareTo(Delayed o) {
      return Long.compare(o.getDelay(TimeUnit.MILLISECONDS), getDelay(TimeUnit.MILLISECONDS));
    }
  }
}
