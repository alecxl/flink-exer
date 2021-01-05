package top.bestcx.flink.chapter07;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * TODO
 *
 * @author cjp
 * @version 1.0
 * @date 2020/12/28 8:43
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class LoginEvent {
    private Long userId;
    private String ip;
    private String eventType;
    private Long eventTime;
}
