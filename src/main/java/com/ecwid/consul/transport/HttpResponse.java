package com.ecwid.consul.transport;

/**
 * @author Vasily Vasilkov (vgv@ecwid.com)
 */
public final class HttpResponse<T> {

	private final int statusCode;
	private final String statusMessage;

	private final T content;

	private final String error;

	private final Long consulIndex;
	private final Boolean consulKnownLeader;
	private final Long consulLastContact;

	public HttpResponse(int statusCode, String statusMessage, T content, Long consulIndex, Boolean consulKnownLeader, Long consulLastContact) {
		this(statusCode, statusMessage, content, consulIndex, consulKnownLeader, consulLastContact, null);
	}

	public HttpResponse(
			int statusCode,
			String statusMessage,
			Long consulIndex,
			Boolean consulKnownLeader,
			Long consulLastContact,
			String error
	) {
		this(statusCode, statusMessage, null, consulIndex, consulKnownLeader, consulLastContact, error);
	}

	public HttpResponse(
			int statusCode,
			String statusMessage,
			T content,
			Long consulIndex,
			Boolean consulKnownLeader,
			Long consulLastContact,
			String error
	) {
		this.statusCode = statusCode;
		this.statusMessage = statusMessage;
		this.content = content;
		this.error = error;
		this.consulIndex = consulIndex;
		this.consulKnownLeader = consulKnownLeader;
		this.consulLastContact = consulLastContact;
	}

	public int getStatusCode() {
		return statusCode;
	}

	public String getStatusMessage() {
		return statusMessage;
	}

	public T getContent() {
		return content;
	}

	public String getError() {
		return error;
	}

	public Long getConsulIndex() {
		return consulIndex;
	}

	public Boolean isConsulKnownLeader() {
		return consulKnownLeader;
	}

	public Long getConsulLastContact() {
		return consulLastContact;
	}
}
