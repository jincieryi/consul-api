package com.ecwid.consul.transport;

import org.apache.http.Header;
import org.apache.http.HttpEntity;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.*;
import org.apache.http.entity.ByteArrayEntity;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.apache.http.util.EntityUtils;

import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.io.UnsupportedEncodingException;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.charset.UnsupportedCharsetException;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Function;

public abstract class AbstractHttpTransport implements HttpTransport {

	static final int DEFAULT_MAX_CONNECTIONS = 1000;
	static final int DEFAULT_MAX_PER_ROUTE_CONNECTIONS = 500;
	static final int DEFAULT_CONNECTION_TIMEOUT = 10 * 1000; // 10 sec

	// 10 minutes for read timeout due to blocking queries timeout
	// https://www.consul.io/api/index.html#blocking-queries
	static final int DEFAULT_READ_TIMEOUT = 1000 * 60 * 10; // 10 min

	@Override
	public <T> HttpResponse<T> makeGetRequest(HttpRequest request, Function<Reader, T> objConverter) {
		HttpGet httpGet = new HttpGet(request.getUrl());
		addHeadersToRequest(httpGet, request.getHeaders());

		return executeRequest(httpGet, objConverter);
	}

	@Override
	public <T> HttpResponse<T> makePutRequest(HttpRequest request, Function<Reader, T> objConverter) {
		HttpPut httpPut = new HttpPut(request.getUrl());
		addHeadersToRequest(httpPut, request.getHeaders());
		if (request.getContent() != null) {
			httpPut.setEntity(new StringEntity(request.getContent(), StandardCharsets.UTF_8));
		} else {
			httpPut.setEntity(new ByteArrayEntity(request.getBinaryContent()));
		}

		return executeRequest(httpPut, objConverter);
	}

	@Override
	public <T> HttpResponse<T> makeDeleteRequest(HttpRequest request, Function<Reader, T> objConverter) {
		HttpDelete httpDelete = new HttpDelete(request.getUrl());
		addHeadersToRequest(httpDelete, request.getHeaders());
		return executeRequest(httpDelete, objConverter);
	}

	/**
	 * You should override this method to instantiate ready to use HttpClient
	 *
	 * @return HttpClient
	 */
	protected abstract HttpClient getHttpClient();

	private <T> HttpResponse<T> executeRequest(HttpUriRequest httpRequest, Function<Reader, T> objConverter) {

		try {
			return getHttpClient().execute(httpRequest, response -> {
				int statusCode = response.getStatusLine().getStatusCode();
				String statusMessage = response.getStatusLine().getReasonPhrase();
				Long consulIndex = parseUnsignedLong(response.getFirstHeader("X-Consul-Index"));
				Boolean consulKnownLeader = parseBoolean(response.getFirstHeader("X-Consul-Knownleader"));
				Long consulLastContact = parseUnsignedLong(response.getFirstHeader("X-Consul-Lastcontact"));
				if (statusCode == 200) {
					HttpEntity entity = response.getEntity();
					Reader inputReader = new InputStreamReader(entity.getContent(), getCharset(entity));
					T value = objConverter.apply(inputReader);
					return new HttpResponse<>(statusCode, statusMessage, value, consulIndex, consulKnownLeader, consulLastContact);
				}
				return new HttpResponse<>(
						statusCode,
						statusMessage,
						consulIndex,
						consulKnownLeader,
						consulLastContact,
						EntityUtils.toString(response.getEntity(), StandardCharsets.UTF_8)
				);
			});
		} catch (IOException e) {
			throw new TransportException(e);
		}
	}

	private Charset getCharset(HttpEntity entity) {
		try {
			ContentType contentType = ContentType.get(entity);
			if (contentType == null) {
				return StandardCharsets.UTF_8;
			}
			Charset charset = contentType.getCharset();
			if (charset == null) {
				return StandardCharsets.UTF_8;
			}
			return charset;
		} catch (UnsupportedCharsetException e) {
			return StandardCharsets.UTF_8;
		}
	}

	private Long parseUnsignedLong(Header header) {
		if (header == null) {
			return null;
		}

		String value = header.getValue();
		if (value == null) {
			return null;
		}

		try {
			return Long.parseUnsignedLong(value);
		} catch (Exception e) {
			return null;
		}
	}

	private Boolean parseBoolean(Header header) {
		if (header == null) {
			return null;
		}

		if ("true".equals(header.getValue())) {
			return true;
		}

		if ("false".equals(header.getValue())) {
			return false;
		}

		return null;
	}

	private void addHeadersToRequest(HttpRequestBase request, Map<String, String> headers) {
		if (headers == null) {
			return;
		}

		for (Map.Entry<String, String> headerValue : headers.entrySet()) {
			String name = headerValue.getKey();
			String value = headerValue.getValue();

			request.addHeader(name, value);
		}
	}
}
