package top.bestcx.flink.chapter07;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * TODO
 *
 * @author cjp
 * @version 1.0
 * @date 2020/12/26 15:25
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class HotAdCountByUserWithWindowEnd {
    private Long userId;
    private Long adId;
    private Long count;
    private Long windowEnd;
}
