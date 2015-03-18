/**
 * 
 */
package net.sf.jabb.util.parallel;

import net.sf.jabb.util.ex.TypedRuntimeException;

/**
 * Exception occurred during the dispatching rather than during the processing.
 * @author James Hu
 *
 */
public class DispatchingException extends TypedRuntimeException{
	private static final long serialVersionUID = -9032220756001235674L;
	public static final int OTHER = 0;
	public static final int SERVER_UNREACHABLE = 1;
	public static final int SERVER_ERROR = 2;
	public static final int SERVER_TIMEOUT = 3;
	
    /** Constructs a new exception with {@code null} as its
     * detail message.  The cause is not initialized, and may subsequently be
     * initialized by a call to {@link #initCause}.
     * @param	type type of the exception
     */
	public DispatchingException(int type){
		super(type);
	}

    /** Constructs a new exception with the specified detail message.
     * The cause is not initialized, and may subsequently be initialized by a
     * call to {@link #initCause}.
     *
     * @param	type type of the exception
     * @param   message   the detail message. The detail message is saved for
     *          later retrieval by the {@link #getMessage()} method.
     */
    public DispatchingException(int type, String message) {
        super(type, message);
    }

    /**
     * Constructs a new exception with the specified detail message and
     * cause.  <p>Note that the detail message associated with
     * {@code cause} is <i>not</i> automatically incorporated in
     * this runtime exception's detail message.
     *
     * @param	type type of the exception
     * @param  message the detail message (which is saved for later retrieval
     *         by the {@link #getMessage()} method).
     * @param  cause the cause (which is saved for later retrieval by the
     *         {@link #getCause()} method).  (A <tt>null</tt> value is
     *         permitted, and indicates that the cause is nonexistent or
     *         unknown.)
     */
    public DispatchingException(int type, String message, Throwable cause) {
        super(type, message, cause);
    }

    /** Constructs a new exception with the specified cause and a
     * detail message of <tt>(cause==null ? null : cause.toString())</tt>
     * (which typically contains the class and detail message of
     * <tt>cause</tt>).  This constructor is useful for runtime exceptions
     * that are little more than wrappers for other throwables.
     *
     * @param	type type of the exception
     * @param  cause the cause (which is saved for later retrieval by the
     *         {@link #getCause()} method).  (A <tt>null</tt> value is
     *         permitted, and indicates that the cause is nonexistent or
     *         unknown.)
     */
    public DispatchingException(int type, Throwable cause) {
        super(type, cause);
    }

}
