package cn.doitedu.yinew.manageplatform.controller;

import cn.doitedu.yinew.manageplatform.pojo.Animal;
import cn.doitedu.yinew.manageplatform.pojo.RuleAtomicParam;
import freemarker.template.Configuration;
import freemarker.template.Template;
import freemarker.template.TemplateExceptionHandler;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.io.File;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.util.ArrayList;
import java.util.HashMap;

@RestController
public class FreeMarkerDemoController {

    @RequestMapping("/demo")
    public String demo(String name, String animalStr) throws Exception {

        String[] split = animalStr.split("-");
        ArrayList<Animal> animals = new ArrayList<>();
        for (String s : split) {
            String[] s1 = s.split("_");
            Animal animal = new Animal(s1[0], Integer.parseInt(s1[1]));
            animals.add(animal);
        }

        // 数据封装
        HashMap<String, Object> data = new HashMap<>();
        data.put("user", name);
        data.put("animals", animals);

        // 构造模板引擎
        Configuration cfg = new Configuration(Configuration.VERSION_2_3_22);
        cfg.setDirectoryForTemplateLoading(new File("manageplatform/src/main/resources/templates"));
        cfg.setDefaultEncoding("UTF-8");
        cfg.setTemplateExceptionHandler(TemplateExceptionHandler.RETHROW_HANDLER);
        Template temp = cfg.getTemplate("demo.ftl");
        Writer out = new OutputStreamWriter(System.out);

        // 调用模板引擎渲染
        temp.process(data, out);

        return "ok";
    }


    @RequestMapping("/cntsql")
    public String getRuleCountSql() throws Exception {
        ArrayList<RuleAtomicParam> params = new ArrayList<>();

        RuleAtomicParam param1 = new RuleAtomicParam();
        param1.setEventId("H");
        HashMap<String, String> prop1 = new HashMap<>();
        prop1.put("p1","v1");
        prop1.put("p2","v3");
        prop1.put("p4","v5");
        param1.setProperties(prop1);


        RuleAtomicParam param2 = new RuleAtomicParam();
        param2.setEventId("C");
        HashMap<String, String> prop2 = new HashMap<>();
        prop2.put("p5","v6");
        param2.setProperties(prop2);


        params.add(param1);
        params.add(param2);


        // 数据封装
        HashMap<String, Object> data = new HashMap<>();
        data.put("events",params);

        Configuration cfg = new Configuration(Configuration.VERSION_2_3_22);
        cfg.setDirectoryForTemplateLoading(new File("manageplatform/src/main/resources/templates"));
        cfg.setDefaultEncoding("UTF-8");
        cfg.setTemplateExceptionHandler(TemplateExceptionHandler.RETHROW_HANDLER);
        Template temp = cfg.getTemplate("eventCountModel.ftl");
        Writer out = new OutputStreamWriter(System.out);

        // 调用模板引擎渲染
        temp.process(data, out);

        return "ok";

    }

}
