package cs.drill.de;

import cs.drill.util.IpUtil;
import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;

@AllArgsConstructor
@Getter
@EqualsAndHashCode
@ToString
public class EnrichedUser {
  private Long startTs;
  private Long endTs;
  private IpUtil.IpPair ip;
  private String user;
}
