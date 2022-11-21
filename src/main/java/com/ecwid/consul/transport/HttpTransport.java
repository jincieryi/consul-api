package com.ecwid.consul.transport;

import java.io.Reader;
import java.util.function.Function;

/**
 * @author Vasily Vasilkov (vgv@ecwid.com)
 */
public interface HttpTransport {

	<T> HttpResponse<T> makeGetRequest(HttpRequest request, Function<Reader, T> objConverter);

	<T> HttpResponse<T> makePutRequest(HttpRequest request, Function<Reader, T> objConverter);

	<T> HttpResponse<T> makeDeleteRequest(HttpRequest request, Function<Reader, T> objConverter);

}
