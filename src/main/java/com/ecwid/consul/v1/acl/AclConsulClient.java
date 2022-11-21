package com.ecwid.consul.v1.acl;

import com.ecwid.consul.ConsulException;
import com.ecwid.consul.SingleUrlParameters;
import com.ecwid.consul.UrlParameters;
import com.ecwid.consul.json.GsonFactory;
import com.ecwid.consul.transport.HttpResponse;
import com.ecwid.consul.transport.TLSConfig;
import com.ecwid.consul.v1.ConsulRawClient;
import com.ecwid.consul.v1.OperationException;
import com.ecwid.consul.v1.Response;
import com.ecwid.consul.v1.acl.model.Acl;
import com.ecwid.consul.v1.acl.model.NewAcl;
import com.ecwid.consul.v1.acl.model.UpdateAcl;
import com.google.gson.reflect.TypeToken;

import java.util.List;
import java.util.Map;

/**
 * @author Vasily Vasilkov (vgv@ecwid.com)
 */
public final class AclConsulClient implements AclClient {

	private final ConsulRawClient rawClient;

	public AclConsulClient(ConsulRawClient rawClient) {
		this.rawClient = rawClient;
	}

	public AclConsulClient() {
		this(new ConsulRawClient());
	}

	public AclConsulClient(TLSConfig tlsConfig) {
		this(new ConsulRawClient(tlsConfig));
	}

	public AclConsulClient(String agentHost) {
		this(new ConsulRawClient(agentHost));
	}

	public AclConsulClient(String agentHost, TLSConfig tlsConfig) {
		this(new ConsulRawClient(agentHost, tlsConfig));
	}

	public AclConsulClient(String agentHost, int agentPort) {
		this(new ConsulRawClient(agentHost, agentPort));
	}

	public AclConsulClient(String agentHost, int agentPort, TLSConfig tlsConfig) {
		this(new ConsulRawClient(agentHost, agentPort, tlsConfig));
	}

	@Override
	public Response<String> aclCreate(NewAcl newAcl, String token) {
		UrlParameters tokenParams = token != null ? new SingleUrlParameters("token", token) : null;
		String json = GsonFactory.getGson().toJson(newAcl);
		HttpResponse<Map<String, String>> httpResponse = rawClient.makePutRequest("/v1/acl/create", json, r -> {
			return GsonFactory.getGson().fromJson(r, new TypeToken<Map<String, String>>() {}.getType());
		}, tokenParams);

		if (httpResponse.getStatusCode() == 200) {
			Map<String, String> value = httpResponse.getContent();
			return new Response<>(value.get("ID"), httpResponse);
		} else {
			throw new OperationException(httpResponse);
		}
	}

	@Override
	public Response<Void> aclUpdate(UpdateAcl updateAcl, String token) {
		UrlParameters tokenParams = token != null ? new SingleUrlParameters("token", token) : null;
		String json = GsonFactory.getGson().toJson(updateAcl);
		HttpResponse<Void> httpResponse = rawClient.makePutRequest("/v1/acl/update", json, r -> null, tokenParams);

		if (httpResponse.getStatusCode() == 200) {
			return new Response<>(null, httpResponse);
		} else {
			throw new OperationException(httpResponse);
		}
	}

	@Override
	public Response<Void> aclDestroy(String aclId, String token) {
		UrlParameters tokenParams = token != null ? new SingleUrlParameters("token", token) : null;
		HttpResponse<Void> httpResponse = rawClient.makePutRequest("/v1/acl/destroy/" + aclId, "", r -> null, tokenParams);

		if (httpResponse.getStatusCode() == 200) {
			return new Response<>(null, httpResponse);
		} else {
			throw new OperationException(httpResponse);
		}
	}

	@Override
	public Response<Acl> getAcl(String id) {
		HttpResponse<List<Acl>> httpResponse = rawClient.makeGetRequest("/v1/acl/info/" + id, r -> {
			return GsonFactory.getGson().fromJson(r, new TypeToken<List<Acl>>() {}.getType());
		});

		if (httpResponse.getStatusCode() == 200) {
			List<Acl> value = httpResponse.getContent();

			if (value.isEmpty()) {
				return new Response<>(null, httpResponse);
			} else if (value.size() == 1) {
				return new Response<>(value.get(0), httpResponse);
			} else {
				throw new ConsulException("Strange response (list size=" + value.size() + ")");
			}
		} else {
			throw new OperationException(httpResponse);
		}
	}

	@Override
	public Response<String> aclClone(String aclId, String token) {
		UrlParameters tokenParams = token != null ? new SingleUrlParameters("token", token) : null;
		HttpResponse<Map<String, String>> httpResponse = rawClient.makePutRequest("/v1/acl/clone/" + aclId, "", r -> {
			return GsonFactory.getGson().fromJson(r, new TypeToken<Map<String, String>>() {}.getType());
		}, tokenParams);

		if (httpResponse.getStatusCode() == 200) {
			Map<String, String> value = httpResponse.getContent();
			return new Response<>(value.get("ID"), httpResponse);
		} else {
			throw new OperationException(httpResponse);
		}
	}

	@Override
	public Response<List<Acl>> getAclList(String token) {
		UrlParameters tokenParams = token != null ? new SingleUrlParameters("token", token) : null;
		HttpResponse<List<Acl>> httpResponse = rawClient.makeGetRequest("/v1/acl/list", r -> {
			return GsonFactory.getGson().fromJson(r, new TypeToken<List<Acl>>() {}.getType());
		}, tokenParams);

		if (httpResponse.getStatusCode() == 200) {
			List<Acl> value = httpResponse.getContent();
			return new Response<>(value, httpResponse);
		} else {
			throw new OperationException(httpResponse);
		}
	}

}
