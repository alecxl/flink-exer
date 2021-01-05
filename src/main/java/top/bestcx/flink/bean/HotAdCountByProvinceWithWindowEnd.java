package top.bestcx.flink.bean;

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
public class HotAdCountByProvinceWithWindowEnd {
    private String province;
    private Long adId;
    private Long count;
    private Long windowEnd;
}
