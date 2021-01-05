package top.bestcx.flink.bean;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * TODO
 *
 * @author cjp
 * @version 1.0
 * @date 2020/12/26 10:32
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class HotItemCountWithWindowEnd {
    private Long itemId;
    private Long itemCount;
    private Long windowEndTs;
}
