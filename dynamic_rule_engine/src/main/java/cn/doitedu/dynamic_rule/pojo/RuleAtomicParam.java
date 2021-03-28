package cn.doitedu.dynamic_rule.pojo;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;
import java.util.HashMap;

/**
 * @author 涛哥
 * @nick_name "deep as the sea"
 * @contact qq:657270652 wx:doit_edu
 * @site www.doitedu.cn
 * @date 2021-03-28
 * @desc 规则参数中的原子条件封装实体
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class RuleAtomicParam implements Serializable {

    // 事件的类型要求
    private String eventId;

    // 事件的属性要求
    private HashMap<String,String> properties;

    // 规则要求的阈值
    private int cnts;

    // 要求的事件发生时间段起始
    private long rangeStart;

    // 要求的事件发生时间段结束
    private long rangeEnd;

    // 用于记录查询服务所返回的查询值
    private int realCnts;


}
