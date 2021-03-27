package cn.doitedu.dynamic_rule.pojo;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.ToString;

import java.util.Map;

@Data
@NoArgsConstructor
@AllArgsConstructor
@ToString
public class LogBean {

    private String account        ;
    private String appId          ;
    private String appVersion     ;
    private String carrier        ;
    private String deviceId       ;
    private String deviceType     ;
    private String ip             ;
    private double latitude       ;
    private double longitude      ;
    private String netType        ;
    private String osName         ;
    private String osVersion      ;
    private String releaseChannel ;
    private String resolution     ;
    private String sessionId      ;
    private long timeStamp        ;
    private String eventId        ;
    private Map<String,String> properties;

}
