package top.bestcx.flink.chapter07;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * TODO
 *
 * @author cjp
 * @version 1.0
 * @date 2020/12/26 13:59
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class ApacheLog {
    private String ip;
    private String userId;
    private Long eventTime;
    private String method;
    private String url;
}
