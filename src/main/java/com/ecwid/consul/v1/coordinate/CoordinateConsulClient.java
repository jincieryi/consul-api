package com.ecwid.consul.v1.coordinate;

import com.ecwid.consul.json.GsonFactory;
import com.ecwid.consul.transport.HttpResponse;
import com.ecwid.consul.v1.ConsulRawClient;
import com.ecwid.consul.v1.OperationException;
import com.ecwid.consul.v1.QueryParams;
import com.ecwid.consul.v1.Response;
import com.ecwid.consul.v1.coordinate.model.Datacenter;
import com.ecwid.consul.v1.coordinate.model.Node;
import com.google.gson.reflect.TypeToken;

import java.util.List;

/**
 * @author Vasily Vasilkov (vgv@ecwid.com)
 */
public class CoordinateConsulClient implements CoordinateClient {

	private final ConsulRawClient rawClient;

	public CoordinateConsulClient(ConsulRawClient rawClient) {
		this.rawClient = rawClient;
	}

	@Override
	public Response<List<Datacenter>> getDatacenters() {
		HttpResponse<List<Datacenter>> httpResponse = rawClient.makeGetRequest("/v1/coordinate/datacenters", r -> {
			return GsonFactory.getGson().fromJson(r, new TypeToken<List<Datacenter>>() {}.getType());
		});

		if (httpResponse.getStatusCode() == 200) {
			List<Datacenter> value = httpResponse.getContent();
			return new Response<>(value, httpResponse);
		} else {
			throw new OperationException(httpResponse);
		}
	}

	@Override
	public Response<List<Node>> getNodes(QueryParams queryParams) {
		HttpResponse<List<Node>> httpResponse = rawClient.makeGetRequest("/v1/coordinate/nodes", r -> {
			return GsonFactory.getGson().fromJson(r, new TypeToken<List<Node>>() {}.getType());
		}, queryParams);

		if (httpResponse.getStatusCode() == 200) {
			List<Node> value = httpResponse.getContent();
			return new Response<>(value, httpResponse);
		} else {
			throw new OperationException(httpResponse);
		}
	}
}
