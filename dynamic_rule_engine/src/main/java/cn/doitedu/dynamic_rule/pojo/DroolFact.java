package cn.doitedu.dynamic_rule.pojo;


import cn.doitedu.dynamic_rule.service.QueryRouterV4;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @author 涛哥
 * @nick_name "deep as the sea"
 * @contact qq:657270652 wx:doit_edu
 * @site www.doitedu.cn
 * @date 2021-04-07
 * @desc 封装要insert到drools kiesession中数据的fact实体
 */

@Data
@NoArgsConstructor
@AllArgsConstructor
public class DroolFact {

    private LogBean logBean;

    private RuleParam ruleParam;

    private QueryRouterV4 queryRouterV4;

    private boolean match;


}
