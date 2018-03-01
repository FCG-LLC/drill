package cs.drill.de;

import lombok.NoArgsConstructor;

import java.util.ArrayList;
import java.util.List;

/**
 * Cache for time ranges and related to them user names.
 */
@NoArgsConstructor
public class TimeUserCache {
  /**
   * Timestamps contain next timestamps. Corresponding index in users list contains username
   * which match to time period started by matching timestamp.
   */
  private final List<Long> timestamps = new ArrayList<>();
  private final List<String> users = new ArrayList<>();

  /**
   * Gets user name for given timestamp.
   * Underneath if provided time ranges contains given timestamp then username for time
   * range is returned.
   * @param timestamp timestamp in millis
   * @return String username or null if not known/unspecified
   */
  public String getUser(long timestamp) {
    if (timestamps.isEmpty()) {
      return null;
    }
    if (timestamp > timestamps.get(timestamps.size() - 1)) {
      return null;
    }
    int index = timestampBisect(timestamp, 0, timestamps.size() - 1);
    return users.get(index);
  }

  /**
   * Adds user for the specified time bucket.
   * Can only add to the end of cache (sorted by buckets).
   * @param startTs time range start in millis
   * @param endTs time range end in millis
   * @param userName username
   * @throws IllegalArgumentException when trying to add range which is not after last added
   */
  public void addNextUser(long startTs, long endTs, String userName) {
    if (timestamps.isEmpty()) {
      addNewRange(startTs, endTs, userName);
    } else {
      int lastTsIndex = timestamps.size() - 1;
      long lastTimestamp = timestamps.get(lastTsIndex);
      if (lastTimestamp < startTs) {
        addNewRange(startTs, endTs, userName);
      } else if (lastTimestamp == startTs) {
        addToExistingRange(endTs, userName, lastTsIndex);
      } else if (lastTimestamp > startTs && startTs >= timestamps.get(lastTsIndex - 1)) {
        updateLastBucket(startTs, endTs, userName, lastTsIndex);
      } else {
        throw new IllegalArgumentException(
          "Cache entry is not the newest one. Cannot update cache."
        );
      }
    }
    verifyLengthOfLists();
  }

  private void addToExistingRange(long endTs, String userName, int lastTsIndex) {
    timestamps.add(endTs);
    users.set(lastTsIndex, userName);
    users.add(null);
  }

  private void updateLastBucket(long startTs, long endTs, String userName, int lastTsIndex) {
    Long lastStartTs = timestamps.get(lastTsIndex - 1);
    Long lastEndTs = timestamps.get(lastTsIndex);

    if (lastStartTs < startTs) {
      // new start is last end now
      timestamps.set(lastTsIndex, startTs);
      lastEndTs = startTs;
    }

    if (lastEndTs < endTs) {
      // if last inserted timestamp is smaller then provided
      // then add next bucket
      timestamps.add(endTs);
      users.set(lastTsIndex, userName);
      users.add(null);
    } else if (lastEndTs > endTs) {
      // if last inserted timestamp is bigger then provided then update entry (e.g. time
      // from future or constant added on the end of last entry)
      timestamps.set(lastTsIndex, endTs);
      users.set(lastTsIndex - 1, userName);
    } else if (lastEndTs == endTs) {
      // if for some case bucket matches update name
      users.set(lastTsIndex - 1, userName);
    }
  }

  private void addNewRange(long startTs, long endTs, String userName) {
    timestamps.add(startTs);
    users.add(userName);
    timestamps.add(endTs);
    users.add(null);
  }

  private void verifyLengthOfLists() {
    if (timestamps.size() != users.size()) {
      throw new IllegalStateException("Cache lists are not consistent");
    }
  }

  private int timestampBisect(long timestamp, int minIndex, int maxIndex) {
    if (maxIndex - minIndex <= 1) {
      return minIndex;
    }

    int middleIndex = minIndex + (maxIndex - minIndex) / 2;
    if (timestamps.get(middleIndex) > timestamp) {
      return timestampBisect(timestamp, minIndex, middleIndex);
    } else {
      return timestampBisect(timestamp, middleIndex, maxIndex);
    }
  }
}
