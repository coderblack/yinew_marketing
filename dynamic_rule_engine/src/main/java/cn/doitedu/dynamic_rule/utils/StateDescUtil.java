package cn.doitedu.dynamic_rule.utils;

import cn.doitedu.dynamic_rule.pojo.LogBean;
import cn.doitedu.dynamic_rule.pojo.RuleStateBean;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.kie.api.runtime.KieSession;

public class StateDescUtil {

    /**
     * 存放drools规则容器session的state定义器
     */
    public static final MapStateDescriptor<String, RuleStateBean> ruleKieStateDesc = new MapStateDescriptor<String, RuleStateBean>("ruleKieState",String.class,RuleStateBean.class);

    public static final ListStateDescriptor<LogBean> eventStateDesc = new ListStateDescriptor<>("eventState", LogBean.class);


}
