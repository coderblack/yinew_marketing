package cn.doitedu.dynamic_rule.pojo;


import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.kie.api.runtime.KieSession;

/**
 * @author 涛哥
 * @nick_name "deep as the sea"
 * @contact qq:657270652 wx:doit_edu
 * @site www.doitedu.cn
 * @date 2021-04-07
 * @desc 用于封装放入state中的规则相关信息
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class RuleStateBean {

    private String ruleName;
    private KieSession kieSession;
    private RuleParam ruleParam;
    private String ruleType;
    // cn.doitedu.dynamic_rule.service.XqueryRouter
    private String routerClass;
    private String cntSqls;
    private String seqSqls;

}
