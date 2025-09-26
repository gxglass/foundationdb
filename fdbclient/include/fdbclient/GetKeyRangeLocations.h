bool checkOnlyEndpointFailed(const Database& cx, const Endpoint& endpoint);

// Get the SS locations for each shard in the 'keys' key-range;
// Returned vector size is the number of shards in the input keys key-range.
// Returned vector element is <ShardRange, storage server location info> pairs, where
// ShardRange is the whole shard key-range, not a part of the given key range.
// Example: If query the function with  key range (b, d), the returned list of pairs could be something like:
// [([a, b1), locationInfo), ([b1, c), locationInfo), ([c, d1), locationInfo)].
template <class F>
Future<std::vector<KeyRangeLocationInfo>> getKeyRangeLocations(Database const& cx,
															   TenantInfo const& tenant,
                                                               KeyRange const& keys,
                                                               int limit,
                                                               Reverse reverse,
                                                               F StorageServerInterface::* member,
                                                               SpanContext const& spanContext,
                                                               Optional<UID> const& debugID,
                                                               UseProvisionalProxies useProvisionalProxies,
                                                               Version version) {

	ASSERT(!keys.empty());

	std::vector<KeyRangeLocationInfo> locations;
	if (!cx->getCachedLocations(tenant, keys, locations, limit, reverse)) {
		return getKeyRangeLocations_internal(
			 cx, tenant, keys, limit, reverse, spanContext, debugID, useProvisionalProxies, version);
	}

	bool foundFailed = false;
	for (const auto& locationInfo : locations) {
		bool onlyEndpointFailedAndNeedRefresh = false;
		for (int i = 0; i < locationInfo.locations->size(); i++) {
			if (checkOnlyEndpointFailed(cx, locationInfo.locations->get(i, member).getEndpoint())) {
				onlyEndpointFailedAndNeedRefresh = true;
			}
		}

		if (onlyEndpointFailedAndNeedRefresh) {
			cx->invalidateCache(tenant.prefix, locationInfo.range.begin);
			foundFailed = true;
		}
	}

	if (foundFailed) {
		// Refresh the cache with a new getKeyRangeLocations made to proxies.
		return getKeyRangeLocations_internal(
		    cx, tenant, keys, limit, reverse, spanContext, debugID, useProvisionalProxies, version);
	}

	return locations;
}

template <class F>
Future<std::vector<KeyRangeLocationInfo>> getKeyRangeLocations(Reference<TransactionState> trState,
                                                               KeyRange const& keys,
                                                               int limit,
                                                               Reverse reverse,
                                                               F StorageServerInterface::* member) {
	return getKeyRangeLocations(trState->cx,
								TenantInfo(),
	                            keys,
	                            limit,
	                            reverse,
	                            member,
	                            trState->spanContext,
	                            trState->readOptions.present() ? trState->readOptions.get().debugID : Optional<UID>(),
	                            trState->useProvisionalProxies,
	                            trState->readVersionFuture.isValid() && trState->readVersionFuture.isReady()
	                                ? trState->readVersion()
	                                : latestVersion);
}

