package top.bestcx.flink.bean;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @Author lizhenchao@atguigu.cn
 * @Date 2020/12/19 14:01
 */

@Data
@AllArgsConstructor
@NoArgsConstructor
public class WaterSensor {
    private String id;  // 传感器的id
    private Long ts;   // 时间戳
    private Integer vc;  // 水位
}
