package cn.doitedu.dynamic_rule.functions;

import cn.doitedu.dynamic_rule.pojo.LogBean;
import org.apache.flink.api.java.functions.KeySelector;

public class DeviceKeySelector implements KeySelector<LogBean,String> {
    @Override
    public String getKey(LogBean value) throws Exception {

        return value.getDeviceId();
    }
}
