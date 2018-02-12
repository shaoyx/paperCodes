package com.graphtools.utils;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

@Retention(value = RetentionPolicy.RUNTIME)
@Target(value = ElementType.TYPE)
public  @interface GraphAnalyticTool {
	  /**
	   * Name of the algorithm.
	   */
	  String name();

	  /**
	   * Short description of algorithm which is going to be presented to the user.
	   */
	  String description() default "";
}
