package cn.doitedu.yinew.manageplatform.pojo;


import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class RuleStatus implements Serializable {
    /**
     * {
     *    ruleName: '运营公众号拉新',
     *    ruleType: '触发型',
     *    publishTime: '2021-04-01 12:30:45',
     *    lastTrigTime: '2021-06-08 13:30:30',
     *    trigCount: 80,
     *    hitCount: 20,
     *    hitRatio: '30%',
     *    compareGroupRatio: '20%',
     *    ruleGroupRatio: '40%',
     *    runStatus:true
     * }
     */

    private String ruleName;
    private String ruleId;
    private String lastTrigTime;
    private String publishTime;
    private String ruleType;
    private String trigCount;
    private String hitRatio;
    private String hitCount;
    private String compareGroupRatio;
    private String ruleGroupRatio;
    private String runStatus;

}
