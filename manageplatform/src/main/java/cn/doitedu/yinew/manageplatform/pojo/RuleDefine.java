package cn.doitedu.yinew.manageplatform.pojo;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.springframework.web.context.annotation.ApplicationScope;

import java.io.Serializable;
import java.util.List;

/***
 * @author hunter.d
 * @qq 657270652
 * @wx haitao-duan
 * @date 2021/5/9
 **/
@Data
@NoArgsConstructor
@AllArgsConstructor
public class RuleDefine implements Serializable {
        private String ruleName;
}
