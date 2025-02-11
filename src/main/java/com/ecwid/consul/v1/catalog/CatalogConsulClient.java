package com.ecwid.consul.v1.catalog;

import com.ecwid.consul.SingleUrlParameters;
import com.ecwid.consul.UrlParameters;
import com.ecwid.consul.json.GsonFactory;
import com.ecwid.consul.transport.HttpResponse;
import com.ecwid.consul.transport.TLSConfig;
import com.ecwid.consul.v1.*;
import com.ecwid.consul.v1.catalog.model.*;
import com.google.gson.reflect.TypeToken;

import java.util.List;
import java.util.Map;

/**
 * @author Vasily Vasilkov (vgv@ecwid.com)
 */
public final class CatalogConsulClient implements CatalogClient {

	private final ConsulRawClient rawClient;

	public CatalogConsulClient(ConsulRawClient rawClient) {
		this.rawClient = rawClient;
	}

	public CatalogConsulClient() {
		this(new ConsulRawClient());
	}

	public CatalogConsulClient(TLSConfig tlsConfig) {
		this(new ConsulRawClient(tlsConfig));
	}

	public CatalogConsulClient(String agentHost) {
		this(new ConsulRawClient(agentHost));
	}

	public CatalogConsulClient(String agentHost, TLSConfig tlsConfig) {
		this(new ConsulRawClient(agentHost, tlsConfig));
	}

	public CatalogConsulClient(String agentHost, int agentPort) {
		this(new ConsulRawClient(agentHost, agentPort));
	}

	public CatalogConsulClient(String agentHost, int agentPort, TLSConfig tlsConfig) {
		this(new ConsulRawClient(agentHost, agentPort, tlsConfig));
	}

	@Override
	public Response<Void> catalogRegister(CatalogRegistration catalogRegistration) {
		return catalogRegister(catalogRegistration, null);
	}

	@Override
	public Response<Void> catalogRegister(CatalogRegistration catalogRegistration, String token) {
		String json = GsonFactory.getGson().toJson(catalogRegistration);
		UrlParameters tokenParam = token != null ? new SingleUrlParameters("token", token) : null;

		HttpResponse<Void> httpResponse = rawClient.makePutRequest("/v1/catalog/register", json, r -> null, tokenParam);
		if (httpResponse.getStatusCode() == 200) {
			return new Response<>(null, httpResponse);
		} else {
			throw new OperationException(httpResponse);
		}
	}

	@Override
	public Response<Void> catalogDeregister(CatalogDeregistration catalogDeregistration) {
		return catalogDeregister(catalogDeregistration, null);
	}

	@Override
	public Response<Void> catalogDeregister(CatalogDeregistration catalogDeregistration, String token) {
		String json = GsonFactory.getGson().toJson(catalogDeregistration);
		UrlParameters tokenParam = token != null ? new SingleUrlParameters("token", token) : null;

		HttpResponse<Void> httpResponse = rawClient.makePutRequest("/v1/catalog/deregister", json, r -> null, tokenParam);
		if (httpResponse.getStatusCode() == 200) {
			return new Response<>(null, httpResponse);
		} else {
			throw new OperationException(httpResponse);
		}
	}

	@Override
	public Response<List<String>> getCatalogDatacenters() {
		HttpResponse<List<String>> httpResponse = rawClient.makeGetRequest("/v1/catalog/datacenters", r -> {
			return GsonFactory.getGson().fromJson(r, new TypeToken<List<String>>() {}.getType());
		});

		if (httpResponse.getStatusCode() == 200) {
			List<String> value = httpResponse.getContent();
			return new Response<>(value, httpResponse);
		} else {
			throw new OperationException(httpResponse);
		}
	}

	@Override
	public Response<List<Node>> getCatalogNodes(QueryParams queryParams) {
		CatalogNodesRequest request = CatalogNodesRequest.newBuilder()
				.setQueryParams(queryParams)
				.build();

		return getCatalogNodes(request);
	}

	@Override
	public Response<List<Node>> getCatalogNodes(CatalogNodesRequest catalogNodesRequest) {
		Request request = Request.Builder.newBuilder()
			.setEndpoint("/v1/catalog/nodes")
			.addUrlParameters(catalogNodesRequest.asUrlParameters())
			.build();

		HttpResponse<List<Node>> httpResponse = rawClient.makeGetRequest(request, r -> {
			return GsonFactory.getGson().fromJson(r, new TypeToken<List<Node>>() {}.getType());
		});

		if (httpResponse.getStatusCode() == 200) {
			List<Node> value = httpResponse.getContent();
			return new Response<>(value, httpResponse);
		} else {
			throw new OperationException(httpResponse);
		}
	}

	@Override
	public Response<Map<String, List<String>>> getCatalogServices(QueryParams queryParams) {
		return getCatalogServices(queryParams, null);
	}

	@Override
	public Response<Map<String, List<String>>> getCatalogServices(QueryParams queryParams, String token) {
		CatalogServicesRequest request = CatalogServicesRequest.newBuilder()
				.setQueryParams(queryParams)
				.setToken(token)
				.build();

		return getCatalogServices(request);
	}

	@Override
	public Response<Map<String, List<String>>> getCatalogServices(CatalogServicesRequest catalogServicesRequest) {
		HttpResponse<Map<String, List<String>>> httpResponse = rawClient.makeGetRequest("/v1/catalog/services", r -> {
			return GsonFactory.getGson().fromJson(r, new TypeToken<Map<String, List<String>>>() {}.getType());
		}, catalogServicesRequest.asUrlParameters());

		if (httpResponse.getStatusCode() == 200) {
			Map<String, List<String>> value = httpResponse.getContent();
			return new Response<>(value, httpResponse);
		} else {
			throw new OperationException(httpResponse);
		}
	}

	@Override
	public Response<List<com.ecwid.consul.v1.catalog.model.CatalogService>> getCatalogService(String serviceName, QueryParams queryParams) {
		return getCatalogService(serviceName, (String) null, queryParams, null);
	}

	@Override
	public Response<List<com.ecwid.consul.v1.catalog.model.CatalogService>> getCatalogService(String serviceName, QueryParams queryParams, String token) {
		return getCatalogService(serviceName, (String) null, queryParams, token);
	}

	@Override
	public Response<List<com.ecwid.consul.v1.catalog.model.CatalogService>> getCatalogService(String serviceName, String tag,
																							  QueryParams queryParams) {
		return getCatalogService(serviceName, tag, queryParams, null);
	}

	@Override
	public Response<List<com.ecwid.consul.v1.catalog.model.CatalogService>> getCatalogService(String serviceName, String tag,
																							  QueryParams queryParams, String token) {
		return getCatalogService(serviceName, new String[]{tag}, queryParams, null);
	}

	@Override
	public Response<List<com.ecwid.consul.v1.catalog.model.CatalogService>> getCatalogService(String serviceName, String[] tag,
	                                                                                          QueryParams queryParams, String token) {
		CatalogServiceRequest request = CatalogServiceRequest.newBuilder()
				.setTags(tag)
				.setQueryParams(queryParams)
				.setToken(token)
				.build();

		return getCatalogService(serviceName, request);
	}

	@Override
	public Response<List<CatalogService>> getCatalogService(String serviceName, CatalogServiceRequest catalogServiceRequest) {
		HttpResponse<List<com.ecwid.consul.v1.catalog.model.CatalogService>> httpResponse = rawClient.makeGetRequest("/v1/catalog/service/" + serviceName, r -> {
			return GsonFactory.getGson().fromJson(r, new TypeToken<List<com.ecwid.consul.v1.catalog.model.CatalogService>>() {}.getType());
		}, catalogServiceRequest.asUrlParameters());

		if (httpResponse.getStatusCode() == 200) {
			List<com.ecwid.consul.v1.catalog.model.CatalogService> value = httpResponse.getContent();
			return new Response<>(value, httpResponse);
		} else {
			throw new OperationException(httpResponse);
		}
	}

	@Override
	public Response<CatalogNode> getCatalogNode(String nodeName, QueryParams queryParams) {
		HttpResponse<CatalogNode> httpResponse = rawClient.makeGetRequest("/v1/catalog/node/" + nodeName, r -> {
			return GsonFactory.getGson().fromJson(r, new TypeToken<CatalogNode>() {}.getType());
		}, queryParams);

		if (httpResponse.getStatusCode() == 200) {
			CatalogNode catalogNode = httpResponse.getContent();
			return new Response<>(catalogNode, httpResponse);
		} else {
			throw new OperationException(httpResponse);
		}
	}

}
