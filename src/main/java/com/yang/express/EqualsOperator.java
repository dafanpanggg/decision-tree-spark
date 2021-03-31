package com.yang.express;

import com.ql.util.express.Operator;
import com.ql.util.express.OperatorOfNumber;

/**
  * 修改阿里开源QLExpress表达式 `==` 的判断逻辑
  * 修改前：null == null 返回：true
  * 修改后：null == null 返回：false
  *
  * @author yangfan
  * @since 2021/3/19
  * @version 1.0.0
  */
public class EqualsOperator extends Operator {
    @Override
    public Boolean executeInner(Object[] objects) {
        return executeInner(objects[0], objects[1]);
    }

    private Boolean executeInner(Object op1, Object op2) {
        if (null != op1 && null != op2) {
            int compareResult;
            if (op1 instanceof Character || op2 instanceof Character) {
                if (op1 instanceof Character && op2 instanceof Character) {
                    return op1.equals(op2);
                }
                if (op1 instanceof Number) {
                    compareResult = OperatorOfNumber.compareNumber((Number) op1, Integer.valueOf((Character) op2));
                    return compareResult == 0;
                }
                if (op2 instanceof Number) {
                    compareResult = OperatorOfNumber.compareNumber(Integer.valueOf((Character) op1), (Number) op2);
                    return compareResult == 0;
                }
            }
            if (op1 instanceof Number && op2 instanceof Number) {
                compareResult = OperatorOfNumber.compareNumber((Number) op1, (Number) op2);
                return compareResult == 0;
            } else {
                return op1.equals(op2);
            }
        } else {
            return false;
        }
    }
}
