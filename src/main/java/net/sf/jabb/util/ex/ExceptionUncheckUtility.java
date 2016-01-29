package net.sf.jabb.util.ex;

import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;

/**
 * from http://stackoverflow.com/questions/27644361/how-can-i-throw-checked-exceptions-from-inside-java-8-streams
 * @author James Hu
 *
 */
public class ExceptionUncheckUtility {

	@FunctionalInterface
	public interface BiConsumerThrowsExceptions<T, U> {
		void accept(T t, U u) throws Exception;
	}

	@FunctionalInterface
	public interface ConsumerThrowsExceptions<T> {
		void accept(T t) throws Exception;
	}

	@FunctionalInterface
	public interface FunctionThrowsExceptions<T, R> {
		R apply(T t) throws Exception;
	}

	@FunctionalInterface
	public interface PredicateThrowsExceptions<T> {
		Boolean test(T t) throws Exception;
	}

	@FunctionalInterface
	public interface SupplierThrowsExceptions<T> {
		T get() throws Exception;
	}

	@FunctionalInterface
	public interface RunnableThrowsExceptions {
		void run() throws Exception;
	}

	/**
	 * Usage example:
	 * <code>.forEach(consumerThrowsUnchecked(name -&gt; System.out.println(Class.forName(name)))); </code> or
	 * <code>.forEach(consumerThrowsUnchecked(ClassNameUtil::println));</code>
	 * 
	 * @param <T>		argument type of the consumer
	 * @param consumer	the original lambda
	 * @return			the wrapped lambda that throws exceptions in an unchecked manner
	 */
	public static <T> Consumer<T> consumerThrowsUnchecked(ConsumerThrowsExceptions<T> consumer) {
		return t -> {
			try {
				consumer.accept(t);
			} catch (Exception exception) {
				throwAsUnchecked(exception);
			}
		};
	}

	/**
	 * Usage example:
	 * <code>.map(functionThrowsUnchecked(name -&gt; Class.forName(name)))</code> or
	 * <code>.map(functionThrowsUnchecked(Class::forName))</code>
	 * 
	 * @param <T>		argument type of the function
	 * @param <R>		return type of the function
	 * @param function	the original lambda
	 * @return			the wrapped lambda that throws exceptions in an unchecked manner
	 */
	public static <T, R> Function<T, R> functionThrowsUnchecked(FunctionThrowsExceptions<T, R> function) {
		return t -> {
			try {
				return function.apply(t);
			} catch (Exception exception) {
				throwAsUnchecked(exception);
				return null;
			}
		};
	}

	/**
	 * Usage example:
	 * <code>.map(predicateThrowsUnchecked(t -&gt; Class.isInstance(t))</code> or
	 * <code>.map(predicateThrowsUnchecked(Class::isInstance))</code>
	 * 
	 * @param <T>		argument type of the function
	 * @param function	the original lambda
	 * @return			the wrapped lambda that throws exceptions in an unchecked manner
	 */
	public static <T> Predicate<T> predicateThrowsUnchecked(PredicateThrowsExceptions<T> function) {
		return t -> {
			try {
				return function.test(t);
			} catch (Exception exception) {
				throwAsUnchecked(exception);
				return false;
			}
		};
	}

	/**
	 * Usage example:
	 * <code>supplierThrowsUnchecked(() -&gt; new StringJoiner(new String(new byte[]{77, 97, 114, 107}, "UTF-8")))</code>
	 * @param <T>		argument type of the function
	 * @param function	the original lambda
	 * @return			the wrapped lambda that throws exceptions in an unchecked manner
	 */
	public static <T> Supplier<T> supplierThrowsUnchecked(SupplierThrowsExceptions<T> function) {
		return () -> {
			try {
				return function.get();
			} catch (Exception exception) {
				throwAsUnchecked(exception);
				return null;
			}
		};
	}

	/**
	 * Execute the lambda and throw exceptions in an unchecked manner
	 * Usage example: <code>runThrowingUnchecked(() -&gt; Class.forName("xxx"));</code>
	 * @param runnable		the original lambda
	 */
	public static void runThrowingUnchecked(RunnableThrowsExceptions runnable) {
		try {
			runnable.run();
		} catch (Exception exception) {
			throwAsUnchecked(exception);
		}
	}

	/**
	 * Execute the lambda and throw exceptions in an unchecked manner
	 * Usage example: <code>getThrowingUnchecked(() -&gt; Class.forName("xxx"));</code>
	 * @param <R>		return type of the function
	 * @param supplier	the original lambda
	 * @return			the result returned by the lambda
	 */
	public static <R> R getThrowingUnchecked(SupplierThrowsExceptions<R> supplier) {
		try {
			return supplier.get();
		} catch (Exception exception) {
			throwAsUnchecked(exception);
			return null;
		}
	}

	/**
	 * Execute the lambda and throw exceptions in an unchecked manner
	 * Usage example: <code>applyThrowingUnchecked(Class::forName, "xxx");</code>
	 * @param <T>		argument type of the function
	 * @param <R>		return type of the function
	 * @param function	the original lambda
	 * @param t			argument to the lambda
	 * @return			the result returned by the lambda
	 */
	public static <T, R> R applyThrowingUnchecked(FunctionThrowsExceptions<T, R> function, T t) {
		try {
			return function.apply(t);
		} catch (Exception exception) {
			throwAsUnchecked(exception);
			return null;
		}
	}

	/**
	 * Execute the lambda and throw exceptions in an unchecked manner
	 * Usage example: <code>testThrowingUnchecked(Class::isInstance, "xxx");</code>
	 * @param <T>		argument type of the function
	 * @param function	the original lambda
	 * @param t			argument to the lambda
	 * @return			the result returned by the lambda
	 */
	public static <T> Boolean testThrowingUnchecked(PredicateThrowsExceptions<T> function, T t) {
		try {
			return function.test(t);
		} catch (Exception exception) {
			throwAsUnchecked(exception);
			return false;
		}
	}

	/**
	 * Execute the lambda and throw exceptions in an unchecked manner
	 * Usage example: <code>acceptThrowingUnchecked(MySystemOut::println, "abc");</code>
	 * @param <T>		argument type of the function
	 * @param function	the original lambda
	 * @param t			argument to the lambda
	 */
	public static <T> void acceptThrowingUnchecked(ConsumerThrowsExceptions<T> function, T t) {
		try {
			function.accept(t);
		} catch (Exception exception) {
			throwAsUnchecked(exception);
		}
	}

	/**
	 * Throw the exception in an unchecked manner
	 * @param exception	the exception
	 * @throws E	the exception
	 */
	@SuppressWarnings("unchecked")
	private static <E extends Throwable> void throwAsUnchecked(Exception exception) throws E {
		throw (E) exception;
	}
}
