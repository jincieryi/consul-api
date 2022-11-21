package com.ecwid.consul.v1.query;

import com.ecwid.consul.json.GsonFactory;
import com.ecwid.consul.transport.HttpResponse;
import com.ecwid.consul.transport.TLSConfig;
import com.ecwid.consul.v1.ConsulRawClient;
import com.ecwid.consul.v1.OperationException;
import com.ecwid.consul.v1.QueryParams;
import com.ecwid.consul.v1.Response;
import com.ecwid.consul.v1.query.model.QueryExecution;
import com.google.gson.reflect.TypeToken;

public final class QueryConsulClient implements QueryClient {

	private final ConsulRawClient rawClient;

	public QueryConsulClient(ConsulRawClient rawClient) { this.rawClient = rawClient; }

	public QueryConsulClient() { this(new ConsulRawClient()); }

	public QueryConsulClient(TLSConfig tlsConfig) { this(new ConsulRawClient(tlsConfig)); }

	public QueryConsulClient(String agentHost) {
		this(new ConsulRawClient(agentHost));
	}

	public QueryConsulClient(String agentHost, TLSConfig tlsConfig) {
		this(new ConsulRawClient(agentHost, tlsConfig));
	}

	public QueryConsulClient(String agentHost, int agentPort) {
		this(new ConsulRawClient(agentHost, agentPort));
	}

	public QueryConsulClient(String agentHost, int agentPort, TLSConfig tlsConfig) {
		this(new ConsulRawClient(agentHost, agentPort, tlsConfig));
	}

	@Override
	public Response<QueryExecution> executePreparedQuery(String uuid, QueryParams queryParams) {
		HttpResponse<QueryExecution> httpResponse = rawClient.makeGetRequest("/v1/query/" + uuid + "/execute", r -> {
			return GsonFactory.getGson().fromJson(r, new TypeToken<QueryExecution>() {}.getType());
		}, queryParams);

		if (httpResponse.getStatusCode() == 200) {
			QueryExecution queryExecution = httpResponse.getContent();
			return new Response<>(queryExecution, httpResponse);
		} else {
			throw new OperationException(httpResponse);
		}
	}
}
