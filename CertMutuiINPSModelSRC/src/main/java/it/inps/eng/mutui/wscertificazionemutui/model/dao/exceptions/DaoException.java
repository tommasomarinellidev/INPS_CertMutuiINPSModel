/**
 * 
 */
package it.inps.eng.mutui.wscertificazionemutui.model.dao.exceptions;

/**
 * @author ADALFONS
 *
 */
public class DaoException extends Exception {

	private static final long serialVersionUID = -9124596237738528930L;

	public DaoException() {
		super();
	}

	public DaoException(String message, Throwable cause) {
		super(message, cause);
	}

	public DaoException(String message) {
		super(message);
	}

	public DaoException(Throwable cause) {
		super(cause);
	}
}
