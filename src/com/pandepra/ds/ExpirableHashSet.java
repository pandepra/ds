package com.pandepra.ds;

import java.util.HashSet;
import java.util.Map;
import java.util.WeakHashMap;
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

  private final BlockingQueue<Expirable> delayedQueue = new DelayQueue<>();
  private final Map<E, Expirable> expirableMap = new WeakHashMap<>();

  public ExpirableHashSet() {}

  public boolean remove(Object o) {
    discardExpired();
    Expirable expirable = expirableMap.remove((E) o);
    expirable.expire();
    return super.remove(o);
  }

  public boolean add(E element) {
    discardExpired();
    return super.add(element);
  }

  public boolean contains(Object o) {
    discardExpired();
    return super.contains(o);
  }

  public boolean add(E element, long ttl, TimeUnit timeUnit) {
    discardExpired();
    Expirable wrapper = new Expirable(element, ttl, timeUnit);
    delayedQueue.add(wrapper);
    expirableMap.put(element, wrapper);
    return super.add(element);
  }

  public long getExpiry(E element) {
    Expirable expirable = expirableMap.get(element);
    return expirable.expireAt();
  }

  public boolean hasExpired(E element) {
    return expirableMap.containsKey(element);
  }

  /*Removes all expired keys from the set*/
  private void discardExpired() {
    Expirable expiredKey;
    while (null != (expiredKey = delayedQueue.poll())) {
      expirableMap.remove(expiredKey.value());
      super.remove(expiredKey.value());
    }
  }

  /** An element that may expire at some point in future. Supports millis resolution */
  private class Expirable implements Delayed {

    private long expireAt;
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

    public void expire() {
      this.expireAt = Long.MIN_VALUE;
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
