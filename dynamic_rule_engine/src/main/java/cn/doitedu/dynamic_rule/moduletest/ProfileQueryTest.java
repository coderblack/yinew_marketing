package cn.doitedu.dynamic_rule.moduletest;

import cn.doitedu.dynamic_rule.pojo.RuleParam;
import cn.doitedu.dynamic_rule.service.UserProfileQueryServiceHbaseImpl;
import org.junit.Test;

import java.io.IOException;
import java.util.HashMap;

/**
 * @author 涛哥
 * @nick_name "deep as the sea"
 * @contact qq:657270652 wx:doit_edu
 * @site www.doitedu.cn
 * @date 2021-03-28
 * @desc 画像条件查询服务模块测试类
 */
public class ProfileQueryTest {

    @Test
    public void testQueryProfile() throws IOException {
        // 构造参数
        HashMap<String, String> userProfileParams = new HashMap<>();
        userProfileParams.put("tag12","v92");
        userProfileParams.put("tag22","v3");

        RuleParam ruleParam = new RuleParam();
        ruleParam.setUserProfileParams(userProfileParams);


        // 构造一个查询服务
        UserProfileQueryServiceHbaseImpl impl = new UserProfileQueryServiceHbaseImpl();
        boolean b = impl.judgeProfileCondition("000645", ruleParam);
        System.out.println(b);

    }

}
