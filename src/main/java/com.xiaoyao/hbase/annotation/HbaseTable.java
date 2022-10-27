package com.xiaoyao.hbase.annotation;

import java.lang.annotation.*;

@Documented
@Retention(RetentionPolicy.RUNTIME)
@Target({ElementType.TYPE, ElementType.ANNOTATION_TYPE})
public @interface HbaseTable {

    String tableName() default "";

    String familyName() default "";

}
