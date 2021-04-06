package cn.doitedu.dynamic_rule.demos;


import org.kie.api.KieServices;
import org.kie.api.runtime.KieContainer;
import org.kie.api.runtime.KieSession;

public class DroolsDemo {

    public static void main(String[] args) {

        KieServices kieServices = KieServices.Factory.get();
        //默认自动加载 META-INF/kmodule.xml
        KieContainer kieContainer = kieServices.getKieClasspathContainer();
        //kmodule.xml 中定义的 ksession name
        KieSession kieSession = kieContainer.newKieSession("all-rules");


        Applicant applicant = new Applicant("康康", 17);

        // 向引擎插入一个fact数据
        kieSession.insert(applicant);
        // 启动规则计算
        kieSession.fireAllRules();

        // 销毁kie会话
        kieSession.dispose();

    }

}


