/**
 * 
 */
package net.sf.jabb.util.script;

import static org.junit.Assert.*;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import javax.script.Bindings;
import javax.script.SimpleBindings;
import javax.script.ScriptEngine;
import javax.script.ScriptEngineManager;

import net.sf.jabb.util.test.RateTestUtility;
import ognl.Ognl;
import ognl.OgnlContext;

import org.junit.Test;
import org.mvel2.MVEL;
import org.springframework.expression.EvaluationContext;
import org.springframework.expression.Expression;
import org.springframework.expression.ExpressionParser;
import org.springframework.expression.spel.standard.SpelExpressionParser;
import org.springframework.expression.spel.support.StandardEvaluationContext;

/**
 * @author James Hu
 *
 */
public class ScriptEvaluationRateTest {
	static int NUM_THREADS = 10;
	static int WARM_UP_SECONDS = 3;
	static int TEST_SECONDS = 15;
	static int LOOP = 10000;
	
	public static class User{
		private String name;
		private int age;
		private String id;
		
		public User(String name, int age, String id){
			this.name = name;
			this.age = age;
			this.id = id;
		}
		
		public String getName() {
			return name;
		}
		public void setName(String name) {
			this.name = name;
		}
		public int getAge() {
			return age;
		}
		public void setAge(int age) {
			this.age = age;
		}
		public String getId() {
			return id;
		}
		public void setId(String id) {
			this.id = id;
		}
	}
	
	String shortString = "This is a short String";
	String longString = "This is a long String: public class ScriptEvaluationRateTest String shortString String longStringtEvaluationRateTest String shortString String lotEvaluationRateTest String shortString String lotEvaluationRateTest String shortString String lo";
	Long longValue = 9082734892734L;
	Integer intValue = 927398;
	User user = new User("Nobody Me", 12, "This is my id");
	
	String correctResult = shortString + (longValue/intValue) + " " + user.name + user.age + longString.substring(longString.indexOf('p')).length();
	
	@Test
	public void testNativeEvaluation() throws Exception {
		RateTestUtility.doRateTest("testNativeEvaluation", NUM_THREADS, 
				WARM_UP_SECONDS, TimeUnit.SECONDS, null, 
				TEST_SECONDS, TimeUnit.SECONDS, endTime -> {
			int i;
			for (i = 0; i < LOOP && System.currentTimeMillis() < endTime; i ++){
				String s = shortString + (longValue/intValue) + " " + user.name + user.age + longString.substring(longString.indexOf('p')).length();
				//assertEquals(correctResult, s);
			}
			return i;
		});
	}

	@Test
	public void testNashornEvaluation() throws Exception {
		ScriptEngine engine = new ScriptEngineManager().getEngineByName("nashorn");
		RateTestUtility.doRateTest("testNashornEvaluation", NUM_THREADS, 
				WARM_UP_SECONDS, TimeUnit.SECONDS, null, 
				TEST_SECONDS, TimeUnit.SECONDS, endTime -> {
			int i;
			for (i = 0; i < LOOP && System.currentTimeMillis() < endTime; i ++){
				Bindings bindings = new SimpleBindings();
				bindings.put("shortString", shortString);
				bindings.put("longString", longString);
				bindings.put("longValue", longValue);
				bindings.put("intValue", intValue);
				bindings.put("user", user);
				String s = null;
				try {
					s = (String) engine.eval("shortString + Math.round(longValue/intValue) + \" \" + user.name + user.age + longString.substring(longString.indexOf('p')).length()", bindings);
				} catch (Exception e) {
					e.printStackTrace();
				}
				//assertEquals(correctResult, s);
			}
			return i;
		});
	}
	
	@Test
	public void testSpelEvaluation() throws Exception {
		ExpressionParser parser = new SpelExpressionParser();
		Expression exp = parser.parseExpression("#shortString + (#longValue/#intValue) + \" \" + #user.name + #user.age + #longString.substring(#longString.indexOf('p')).length()");
		RateTestUtility.doRateTest("testSpelEvaluation", NUM_THREADS, 
				WARM_UP_SECONDS, TimeUnit.SECONDS, null, 
				TEST_SECONDS, TimeUnit.SECONDS, endTime -> {
			int i;
			for (i = 0; i < LOOP && System.currentTimeMillis() < endTime; i ++){
				EvaluationContext context = new StandardEvaluationContext();
				context.setVariable("shortString", shortString);
				context.setVariable("longString", longString);
				context.setVariable("longValue", longValue);
				context.setVariable("intValue", intValue);
				context.setVariable("user", user);
				String s = null;
				try {
					s = (String) exp.getValue(context);
				} catch (Exception e) {
					e.printStackTrace();
				}
				//assertEquals(correctResult, s);
			}
			return i;
		});
	}
	
	@Test
	public void testOgnlEvaluation() throws Exception {
		Object exp = Ognl.parseExpression("#shortString + (#longValue/#intValue) + \" \" + #user.name + #user.age + #longString.substring(#longString.indexOf('p')).length()");
		RateTestUtility.doRateTest("testOgnlEvaluation", NUM_THREADS, 
				WARM_UP_SECONDS, TimeUnit.SECONDS, null, 
				TEST_SECONDS, TimeUnit.SECONDS, endTime -> {
			int i;
			for (i = 0; i < LOOP && System.currentTimeMillis() < endTime; i ++){
				OgnlContext context = new OgnlContext();
				context.put("shortString", shortString);
				context.put("longString", longString);
				context.put("longValue", longValue);
				context.put("intValue", intValue);
				context.put("user", user);
				String s = null;
				try {
					s = (String) Ognl.getValue(exp, context, null, String.class);
				} catch (Exception e) {
					e.printStackTrace();
				}
				//assertEquals(correctResult, s);
			}
			return i;
		});
	}
	
	@Test
	public void testMvelEvaluation() throws Exception {
		Serializable compiled = MVEL.compileExpression("shortString + (longValue/intValue) + \" \" + user.name + user.age + longString.substring(longString.indexOf('p')).length()");
		RateTestUtility.doRateTest("testMvelEvaluation", NUM_THREADS, 
				WARM_UP_SECONDS, TimeUnit.SECONDS, null, 
				TEST_SECONDS, TimeUnit.SECONDS, endTime -> {
			int i;
			for (i = 0; i < LOOP && System.currentTimeMillis() < endTime; i ++){
				Map<String, Object> vars = new HashMap<>();
				vars.put("shortString", shortString);
				vars.put("longString", longString);
				vars.put("longValue", longValue);
				vars.put("intValue", intValue);
				vars.put("user", user);
				String s = null;
				try {
					s = (String) MVEL.executeExpression(compiled, vars);
				} catch (Exception e) {
					e.printStackTrace();
				}
				//assertEquals(correctResult, s);
			}
			return i;
		});
	}
	

}
