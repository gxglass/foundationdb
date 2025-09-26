/*
 * ChangeFeedRelated.actor.cpp
 * (split out from NativeAPI.actor.cpp)
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2013-2024 Apple Inc. and the FoundationDB project authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

// TODO: prune down the list of includes. This was copied from NativeAPI.actor.cpp.
#include "fdbclient/NativeAPI.actor.h"

#include <algorithm>
#include <cstdio>
#include <iterator>
#include <limits>
#include <memory>
#include <regex>
#include <string>
#include <unordered_set>
#include <tuple>
#include <utility>
#include <vector>

#include "boost/algorithm/string.hpp"

#include "fdbclient/Knobs.h"
#include "flow/CodeProbe.h"
#include "fmt/format.h"

#include "fdbclient/FDBOptions.g.h"
#include "fdbclient/FDBTypes.h"
#include "fdbrpc/FailureMonitor.h"
#include "fdbrpc/MultiInterface.h"
#include "fdbrpc/TenantInfo.h"

#include "fdbclient/ActorLineageProfiler.h"
#include "fdbclient/AnnotateActor.h"
#include "fdbclient/Atomic.h"
#include "fdbclient/BlobGranuleCommon.h"
#include "fdbclient/BlobGranuleRequest.actor.h"
#include "fdbclient/ClusterInterface.h"
#include "fdbclient/ClusterConnectionFile.h"
#include "fdbclient/ClusterConnectionMemoryRecord.h"
#include "fdbclient/CoordinationInterface.h"
#include "fdbclient/CommitTransaction.h"
#include "fdbclient/DatabaseContext.h"
#include "fdbclient/GlobalConfig.actor.h"
#include "fdbclient/IKnobCollection.h"
#include "fdbclient/JsonBuilder.h"
#include "fdbclient/KeyBackedTypes.actor.h"
#include "fdbclient/KeyRangeMap.h"
#include "fdbclient/ManagementAPI.actor.h"
#include "fdbclient/NameLineage.h"
#include "fdbclient/CommitProxyInterface.h"
#include "fdbclient/MonitorLeader.h"
#include "fdbclient/MutationList.h"
#include "fdbclient/ParallelStream.actor.h"
#include "fdbclient/ReadYourWrites.h"
#include "fdbclient/SpecialKeySpace.actor.h"
#include "fdbclient/StorageServerInterface.h"
#include "fdbclient/SystemData.h"
#include "fdbclient/Tenant.h"
#include "fdbclient/TenantSpecialKeys.actor.h"
#include "fdbclient/TransactionLineage.h"
#include "fdbclient/versions.h"
#include "fdbrpc/WellKnownEndpoints.h"
#include "fdbrpc/LoadBalance.h"
#include "fdbrpc/Net2FileSystem.h"
#include "fdbrpc/simulator.h"
#include "fdbrpc/sim_validation.h"
#include "flow/Arena.h"
#include "flow/ActorCollection.h"
#include "flow/DeterministicRandom.h"
#include "flow/Error.h"
#include "flow/FastRef.h"
#include "flow/GetSourceVersion.h"
#include "flow/IRandom.h"
#include "flow/Trace.h"
#include "flow/ProtocolVersion.h"
#include "flow/flow.h"
#include "flow/genericactors.actor.h"
#include "flow/Knobs.h"
#include "flow/Platform.h"
#include "flow/SystemMonitor.h"
#include "flow/TLSConfig.actor.h"
#include "fdbclient/Tracing.h"
#include "flow/UnitTest.h"
#include "flow/network.h"
#include "flow/serialize.h"

#ifdef ADDRESS_SANITIZER
#include <sanitizer/lsan_interface.h>
#endif

#ifdef WIN32
#define WIN32_LEAN_AND_MEAN
#include <Windows.h>
#undef min
#undef max
#else
#include <time.h>
#endif
#include "flow/actorcompiler.h" // This must be the last #include.

// This file: stuff formerly in NativeAPI.actor.cpp that seems related to change feed.


// FIXME: this has undesired head-of-line-blocking behavior in the case of large version jumps.
// For example, say that The current feed version is 100, and one waiter wants to wait for the feed version >= 1000.
// This will send a request with minVersion=1000. Then say someone wants to wait for feed version >= 200. Because we've
// already blocked this updater on version 1000, even if the feed would already be at version 200+, we won't get an
// empty version response until version 1000.
ACTOR Future<Void> storageFeedVersionUpdater(StorageServerInterface interf, ChangeFeedStorageData* self) {
	loop {
		if (self->version.get() < self->desired.get()) {
			wait(delay(CLIENT_KNOBS->CHANGE_FEED_EMPTY_BATCH_TIME) || self->version.whenAtLeast(self->desired.get()));
			if (self->version.get() < self->desired.get()) {
				try {
					ChangeFeedVersionUpdateReply rep = wait(brokenPromiseToNever(
					    interf.changeFeedVersionUpdate.getReply(ChangeFeedVersionUpdateRequest(self->desired.get()))));
					if (rep.version > self->version.get()) {
						self->version.set(rep.version);
					}
				} catch (Error& e) {
					if (e.code() != error_code_server_overloaded) {
						throw;
					}
					if (FLOW_KNOBS->PREVENT_FAST_SPIN_DELAY > CLIENT_KNOBS->CHANGE_FEED_EMPTY_BATCH_TIME) {
						wait(delay(FLOW_KNOBS->PREVENT_FAST_SPIN_DELAY - CLIENT_KNOBS->CHANGE_FEED_EMPTY_BATCH_TIME));
					}
				}
			}
		} else {
			wait(self->desired.whenAtLeast(self->version.get() + 1));
		}
	}
}

ACTOR Future<Void> changeFeedCommitter(IKeyValueStore* storage,
                                       Reference<AsyncVar<bool>> commitChangeFeedStorage,
                                       int64_t* uncommittedCFBytes) {
	loop {
		while (!commitChangeFeedStorage->get()) {
			wait(commitChangeFeedStorage->onChange());
		}
		*uncommittedCFBytes = 0;
		commitChangeFeedStorage->set(false);
		wait(storage->commit());
	}
}

ACTOR Future<Void> cleanupChangeFeedCache(DatabaseContext* db) {
	wait(db->initializeChangeFeedCache);
	wait(delay(CLIENT_KNOBS->CHANGE_FEED_CACHE_EXPIRE_TIME));
	loop {
		for (auto it = db->changeFeedCaches.begin(); it != db->changeFeedCaches.end(); ++it) {
			if (!it->second->active && now() - it->second->inactiveTime > CLIENT_KNOBS->CHANGE_FEED_CACHE_EXPIRE_TIME) {
				Key beginKey = changeFeedCacheKey(it->first.tenantPrefix, it->first.rangeId, it->first.range, 0);
				Key endKey =
				    changeFeedCacheKey(it->first.tenantPrefix, it->first.rangeId, it->first.range, MAX_VERSION);
				db->storage->clear(KeyRangeRef(beginKey, endKey));
				KeyRange feedRange =
				    singleKeyRange(changeFeedCacheFeedKey(it->first.tenantPrefix, it->first.rangeId, it->first.range));
				db->storage->clear(feedRange);

				db->uncommittedCFBytes += beginKey.size() + endKey.size() + feedRange.expectedSize();
				if (db->uncommittedCFBytes > CLIENT_KNOBS->CHANGE_FEED_CACHE_FLUSH_BYTES) {
					db->commitChangeFeedStorage->set(true);
				}

				auto& rangeIdCache = db->rangeId_cacheData[it->first.rangeId];
				rangeIdCache.erase(it->first);
				if (rangeIdCache.empty()) {
					db->rangeId_cacheData.erase(it->first.rangeId);
				}
				db->changeFeedCaches.erase(it);
				break;
			}
		}
		wait(delay(5.0));
	}
}

ACTOR Future<Void> initializeCFCache(DatabaseContext* db) {
	state Key beginKey = changeFeedCacheFeedKeys.begin;
	loop {
		RangeResult res = wait(db->storage->readRange(KeyRangeRef(beginKey, changeFeedCacheFeedKeys.end),
		                                              CLIENT_KNOBS->CHANGE_FEED_CACHE_LIMIT_BYTES,
		                                              CLIENT_KNOBS->CHANGE_FEED_CACHE_LIMIT_BYTES));
		if (res.size()) {
			beginKey = keyAfter(res.back().key);
		} else {
			ASSERT(!res.more);
		}
		for (auto& kv : res) {
			ChangeFeedCacheRange cf(decodeChangeFeedCacheFeedKey(kv.key));
			Reference<ChangeFeedCacheData> data = makeReference<ChangeFeedCacheData>();
			auto val = decodeChangeFeedCacheFeedValue(kv.value);
			data->version = val.first;
			data->popped = val.second;
			data->active = false;
			data->inactiveTime = now();
			db->changeFeedCaches[cf] = data;
			db->rangeId_cacheData[cf.rangeId][cf] = data;
		}
		if (!res.more) {
			break;
		}
	}
	return Void();
}


// Because two storage servers, depending on the shard map, can have different representations of a clear at the same
// version depending on their shard maps at the time of the mutation, it is non-trivial to directly compare change feed
// streams. Instead we compare the presence of data at each version. This both saves on cpu cost of validation, and
// because historically most change feed corruption bugs are the absence of entire versions, not a subset of mutations
// within a version.
struct ChangeFeedTSSValidationData {
	PromiseStream<Version> ssStreamSummary;
	ReplyPromiseStream<ChangeFeedStreamReply> tssStream;
	Future<Void> validatorFuture;
	std::deque<std::pair<Version, Version>> rollbacks;
	Version popVersion = invalidVersion;
	bool done = false;

	ChangeFeedTSSValidationData() {}
	ChangeFeedTSSValidationData(ReplyPromiseStream<ChangeFeedStreamReply> tssStream) : tssStream(tssStream) {}

	void updatePopped(Version newPopVersion) { popVersion = std::max(popVersion, newPopVersion); }

	bool checkRollback(const MutationsAndVersionRef& m) {
		if (m.mutations.size() == 1 && m.mutations.back().param1 == lastEpochEndPrivateKey) {
			if (rollbacks.empty() || rollbacks.back().second < m.version) {
				Version rollbackVersion;
				BinaryReader br(m.mutations.back().param2, Unversioned());
				br >> rollbackVersion;
				if (!rollbacks.empty()) {
					ASSERT(rollbacks.back().second <= rollbackVersion);
				}
				rollbacks.push_back({ rollbackVersion, m.version });
			}
			return true;
		} else {
			return false;
		}
	}

	bool shouldAddMutation(const MutationsAndVersionRef& m) {
		return !done && !m.mutations.empty() && !checkRollback(m);
	}

	bool isRolledBack(Version v) {
		if (rollbacks.empty()) {
			return false;
		}
		for (int i = 0; i < rollbacks.size(); i++) {
			if (v <= rollbacks[i].first) {
				return false;
			}
			if (v < rollbacks[i].second) {
				return true;
			}
		}
		return false;
	}

	void send(const ChangeFeedStreamReply& ssReply) {
		if (done) {
			return;
		}
		updatePopped(ssReply.popVersion);
		for (auto& it : ssReply.mutations) {
			if (shouldAddMutation(it)) {
				ssStreamSummary.send(it.version);
			}
		}
	}

	void complete() {
		done = true;
		// destroy TSS stream to stop server actor
		tssStream.reset();
	}
};
void handleTSSChangeFeedMismatch(const ChangeFeedStreamRequest& request,
                                 const TSSEndpointData& tssData,
                                 int64_t matchesFound,
                                 Version lastMatchingVersion,
                                 Version ssVersion,
                                 Version tssVersion,
                                 Version popVersion) {
	if (request.canReadPopped) {
		// There is a known issue where this can return different data between an SS and TSS when a feed was popped but
		// the SS restarted before the pop could be persisted, for reads that can read popped data. As such, only count
		// this as a mismatch when !req.canReadPopped
		return;
	}
	CODE_PROBE(true, "TSS mismatch in stream comparison");

	if (tssData.metrics->shouldRecordDetailedMismatch()) {
		TraceEvent mismatchEvent(
		    (g_network->isSimulated() && g_simulator->tssMode == ISimulator::TSSMode::EnabledDropMutations)
		        ? SevWarnAlways
		        : SevError,
		    "TSSMismatchChangeFeedStream");
		mismatchEvent.setMaxEventLength(FLOW_KNOBS->TSS_LARGE_TRACE_SIZE);

		// request info
		mismatchEvent.detail("TSSID", tssData.tssId);
		mismatchEvent.detail("FeedID", request.rangeID);
		mismatchEvent.detail("BeginVersion", request.begin);
		mismatchEvent.detail("EndVersion", request.end);
		mismatchEvent.detail("StartKey", request.range.begin);
		mismatchEvent.detail("EndKey", request.range.end);
		mismatchEvent.detail("CanReadPopped", request.canReadPopped);
		mismatchEvent.detail("PopVersion", popVersion);
		mismatchEvent.detail("DebugUID", request.id);

		// mismatch info
		mismatchEvent.detail("MatchesFound", matchesFound);
		mismatchEvent.detail("LastMatchingVersion", lastMatchingVersion);
		mismatchEvent.detail("SSVersion", ssVersion);
		mismatchEvent.detail("TSSVersion", tssVersion);

		CODE_PROBE(FLOW_KNOBS->LOAD_BALANCE_TSS_MISMATCH_TRACE_FULL,
		           "Tracing Full TSS Feed Mismatch in stream comparison",
		           probe::decoration::rare);
		CODE_PROBE(!FLOW_KNOBS->LOAD_BALANCE_TSS_MISMATCH_TRACE_FULL,
		           "Tracing Partial TSS Feed Mismatch in stream comparison and storing the rest in FDB");

		if (!FLOW_KNOBS->LOAD_BALANCE_TSS_MISMATCH_TRACE_FULL) {
			mismatchEvent.disable();
			UID mismatchUID = deterministicRandom()->randomUniqueID();
			tssData.metrics->recordDetailedMismatchData(mismatchUID, mismatchEvent.getFields().toString());

			// record a summarized trace event instead
			TraceEvent summaryEvent(
			    (g_network->isSimulated() && g_simulator->tssMode == ISimulator::TSSMode::EnabledDropMutations)
			        ? SevWarnAlways
			        : SevError,
			    "TSSMismatchChangeFeedStream");
			summaryEvent.detail("TSSID", tssData.tssId)
			    .detail("MismatchId", mismatchUID)
			    .detail("FeedDebugUID", request.id);
		}
	}
}

ACTOR Future<Void> changeFeedTSSValidator(ChangeFeedStreamRequest req,
                                          Optional<ChangeFeedTSSValidationData>* data,
                                          TSSEndpointData tssData) {
	state bool ssDone = false;
	state bool tssDone = false;
	state std::deque<Version> ssSummary;
	state std::deque<Version> tssSummary;

	ASSERT(data->present());
	state int64_t matchesFound = 0;
	state Version lastMatchingVersion = req.begin - 1;

	loop {
		// If SS stream gets error, whole stream data gets reset, so it's ok to cancel this actor
		if (!ssDone && ssSummary.empty()) {
			try {
				Version next = waitNext(data->get().ssStreamSummary.getFuture());
				ssSummary.push_back(next);
			} catch (Error& e) {
				if (e.code() == error_code_actor_cancelled) {
					throw;
				}
				if (e.code() != error_code_end_of_stream) {
					data->get().complete();
					if (e.code() != error_code_operation_cancelled) {
						tssData.metrics->ssError(e.code());
					}
					throw e;
				}
				ssDone = true;
				if (tssDone) {
					data->get().complete();
					return Void();
				}
			}
		}

		if (!tssDone && tssSummary.empty()) {
			try {
				choose {
					when(ChangeFeedStreamReply nextTss = waitNext(data->get().tssStream.getFuture())) {
						data->get().updatePopped(nextTss.popVersion);
						for (auto& it : nextTss.mutations) {
							if (data->get().shouldAddMutation(it)) {
								tssSummary.push_back(it.version);
							}
						}
					}
					// if ss has result, tss needs to return it
					when(wait((ssDone || !ssSummary.empty()) ? delay(2.0 * FLOW_KNOBS->LOAD_BALANCE_TSS_TIMEOUT)
					                                         : Never())) {
						++tssData.metrics->tssTimeouts;
						data->get().complete();
						return Void();
					}
				}

			} catch (Error& e) {
				if (e.code() == error_code_operation_cancelled) {
					throw e;
				}
				if (e.code() == error_code_end_of_stream) {
					tssDone = true;
					if (ssDone) {
						data->get().complete();
						return Void();
					}
				} else {
					tssData.metrics->tssError(e.code());
					data->get().complete();
					return Void();
				}
			}
		}

		// handle rollbacks and concurrent pops
		while (!ssSummary.empty() &&
		       (ssSummary.front() < data->get().popVersion || data->get().isRolledBack(ssSummary.front()))) {
			ssSummary.pop_front();
		}

		while (!tssSummary.empty() &&
		       (tssSummary.front() < data->get().popVersion || data->get().isRolledBack(tssSummary.front()))) {
			tssSummary.pop_front();
		}
		while (!ssSummary.empty() && !tssSummary.empty()) {
			CODE_PROBE(true, "Comparing TSS change feed data");
			if (ssSummary.front() != tssSummary.front()) {
				CODE_PROBE(true, "TSS change feed mismatch");
				handleTSSChangeFeedMismatch(req,
				                            tssData,
				                            matchesFound,
				                            lastMatchingVersion,
				                            ssSummary.front(),
				                            tssSummary.front(),
				                            data->get().popVersion);
				data->get().complete();
				return Void();
			}
			matchesFound++;
			lastMatchingVersion = ssSummary.front();
			ssSummary.pop_front();
			tssSummary.pop_front();

			while (!data->get().rollbacks.empty() && data->get().rollbacks.front().second <= lastMatchingVersion) {
				data->get().rollbacks.pop_front();
			}
		}

		ASSERT(!ssDone || !tssDone); // both shouldn't be done, otherwise we shouldn't have looped
		if ((ssDone && !tssSummary.empty()) || (tssDone && !ssSummary.empty())) {
			CODE_PROBE(true, "TSS change feed mismatch at end of stream");
			handleTSSChangeFeedMismatch(req,
			                            tssData,
			                            matchesFound,
			                            lastMatchingVersion,
			                            ssDone ? -1 : ssSummary.front(),
			                            tssDone ? -1 : tssSummary.front(),
			                            data->get().popVersion);
			data->get().complete();
			return Void();
		}
	}
}

void maybeDuplicateTSSChangeFeedStream(ChangeFeedStreamRequest& req,
                                       const RequestStream<ChangeFeedStreamRequest>& stream,
                                       QueueModel* model,
                                       Optional<ChangeFeedTSSValidationData>* tssData) {
	if (model) {
		Optional<TSSEndpointData> tssPair = model->getTssData(stream.getEndpoint().token.first());
		if (tssPair.present()) {
			CODE_PROBE(true, "duplicating feed stream to TSS");
			resetReply(req);

			RequestStream<ChangeFeedStreamRequest> tssRequestStream(tssPair.get().endpoint);
			*tssData = Optional<ChangeFeedTSSValidationData>(
			    ChangeFeedTSSValidationData(tssRequestStream.getReplyStream(req)));
			// tie validator actor to the lifetime of the stream being active
			tssData->get().validatorFuture = changeFeedTSSValidator(req, tssData, tssPair.get());
		}
	}
}

ChangeFeedStorageData::~ChangeFeedStorageData() {
	if (context) {
		context->changeFeedUpdaters.erase(interfToken);
	}
}

ChangeFeedData::ChangeFeedData(DatabaseContext* context)
  : dbgid(deterministicRandom()->randomUniqueID()), context(context), notAtLatest(1), created(now()) {
	if (context) {
		context->notAtLatestChangeFeeds[dbgid] = this;
	}
}
ChangeFeedData::~ChangeFeedData() {
	if (context) {
		context->notAtLatestChangeFeeds.erase(dbgid);
	}
}

Version ChangeFeedData::getVersion() {
	return lastReturnedVersion.get();
}

// This function is essentially bubbling the information about what has been processed from the server through the
// change feed client. First it makes sure the server has returned all mutations up through the target version, the
// native api has consumed and processed, them, and then the fdb client has consumed all of the mutations.
ACTOR Future<Void> changeFeedWaitLatest(Reference<ChangeFeedData> self, Version version) {
	// wait on SS to have sent up through version
	std::vector<Future<Void>> allAtLeast;
	for (auto& it : self->storageData) {
		if (it->version.get() < version) {
			if (version > it->desired.get()) {
				it->desired.set(version);
			}
			allAtLeast.push_back(it->version.whenAtLeast(version));
		}
	}

	wait(waitForAll(allAtLeast));

	// then, wait on ss streams to have processed up through version
	std::vector<Future<Void>> onEmpty;
	for (auto& it : self->streams) {
		if (!it.isEmpty()) {
			onEmpty.push_back(it.onEmpty());
		}
	}

	if (onEmpty.size()) {
		wait(waitForAll(onEmpty));
	}

	if (self->mutations.isEmpty()) {
		wait(delay(0));
	}

	// wait for merge cursor to fully process everything it read from its individual promise streams, either until it is
	// done processing or we have up through the desired version
	while (self->lastReturnedVersion.get() < self->maxSeenVersion && self->lastReturnedVersion.get() < version) {
		Version target = std::min(self->maxSeenVersion, version);
		wait(self->lastReturnedVersion.whenAtLeast(target));
	}

	// then, wait for client to have consumed up through version
	if (self->maxSeenVersion >= version) {
		// merge cursor may have something buffered but has not yet sent it to self->mutations, just wait for
		// lastReturnedVersion
		wait(self->lastReturnedVersion.whenAtLeast(version));
	} else {
		// all mutations <= version are in self->mutations, wait for empty
		while (!self->mutations.isEmpty()) {
			wait(self->mutations.onEmpty());
			wait(delay(0));
		}
	}

	return Void();
}

ACTOR Future<Void> changeFeedWhenAtLatest(Reference<ChangeFeedData> self, Version version) {
	if (version >= self->endVersion) {
		return Never();
	}
	if (version <= self->getVersion()) {
		return Void();
	}
	state Future<Void> lastReturned = self->lastReturnedVersion.whenAtLeast(version);
	loop {
		// only allowed to use empty versions if you're caught up
		Future<Void> waitEmptyVersion = (self->notAtLatest.get() == 0) ? changeFeedWaitLatest(self, version) : Never();
		choose {
			when(wait(waitEmptyVersion)) {
				break;
			}
			when(wait(lastReturned)) {
				break;
			}
			when(wait(self->refresh.getFuture())) {}
			when(wait(self->notAtLatest.onChange())) {}
		}
	}

	if (self->lastReturnedVersion.get() < version) {
		self->lastReturnedVersion.set(version);
	}
	ASSERT(self->getVersion() >= version);
	return Void();
}

Future<Void> ChangeFeedData::whenAtLeast(Version version) {
	return changeFeedWhenAtLatest(Reference<ChangeFeedData>::addRef(this), version);
}

#define DEBUG_CF_CLIENT_TRACE false

ACTOR Future<Void> partialChangeFeedStream(StorageServerInterface interf,
                                           PromiseStream<Standalone<MutationsAndVersionRef>> results,
                                           ReplyPromiseStream<ChangeFeedStreamReply> replyStream,
                                           Version begin,
                                           Version end,
                                           Reference<ChangeFeedData> feedData,
                                           Reference<ChangeFeedStorageData> storageData,
                                           UID debugUID,
                                           Optional<ChangeFeedTSSValidationData>* tssData) {

	// calling lastReturnedVersion's callbacks could cause us to be cancelled
	state Promise<Void> refresh = feedData->refresh;
	state bool atLatestVersion = false;
	state Version nextVersion = begin;
	// We don't need to force every other partial stream to do an empty if we get an empty, but if we get actual
	// mutations back after sending an empty, we may need the other partial streams to get an empty, to advance the
	// merge cursor, so we can send the mutations we just got.
	// if lastEmpty != invalidVersion, we need to update the desired versions of the other streams BEFORE waiting
	// onReady once getting a reply
	state Version lastEmpty = invalidVersion;
	try {
		loop {
			if (nextVersion >= end) {
				results.sendError(end_of_stream());
				return Void();
			}
			choose {
				when(state ChangeFeedStreamReply rep = waitNext(replyStream.getFuture())) {
					// handle first empty mutation on stream establishment explicitly
					if (nextVersion == begin && rep.mutations.size() == 1 && rep.mutations[0].mutations.size() == 0 &&
					    rep.mutations[0].version == begin - 1) {
						continue;
					}

					if (DEBUG_CF_CLIENT_TRACE) {
						TraceEvent(SevDebug, "TraceChangeFeedClientMergeCursorReply", debugUID)
						    .detail("SSID", storageData->id)
						    .detail("AtLatest", atLatestVersion)
						    .detail("FirstVersion", rep.mutations.front().version)
						    .detail("LastVersion", rep.mutations.back().version)
						    .detail("Count", rep.mutations.size())
						    .detail("MinStreamVersion", rep.minStreamVersion)
						    .detail("PopVersion", rep.popVersion)
						    .detail("RepAtLatest", rep.atLatestVersion);
					}

					if (rep.mutations.back().version > feedData->maxSeenVersion) {
						feedData->maxSeenVersion = rep.mutations.back().version;
					}
					if (rep.popVersion > feedData->popVersion) {
						feedData->popVersion = rep.popVersion;
					}
					if (tssData->present()) {
						tssData->get().updatePopped(rep.popVersion);
					}

					if (lastEmpty != invalidVersion && !results.isEmpty()) {
						for (auto& it : feedData->storageData) {
							if (refresh.canBeSet() && lastEmpty > it->desired.get()) {
								it->desired.set(lastEmpty);
							}
						}
						lastEmpty = invalidVersion;
					}

					state int resultLoc = 0;
					while (resultLoc < rep.mutations.size()) {
						wait(results.onEmpty());
						if (rep.mutations[resultLoc].version >= nextVersion) {
							if (tssData->present() && tssData->get().shouldAddMutation(rep.mutations[resultLoc])) {
								tssData->get().ssStreamSummary.send(rep.mutations[resultLoc].version);
							}

							results.send(rep.mutations[resultLoc]);

							if (DEBUG_CF_CLIENT_TRACE) {
								TraceEvent(SevDebug, "TraceChangeFeedClientMergeCursorSend", debugUID)
								    .detail("Version", rep.mutations[resultLoc].version)
								    .detail("Size", rep.mutations[resultLoc].mutations.size());
							}

							// check refresh.canBeSet so that, if we are killed after calling one of these callbacks, we
							// just skip to the next wait and get actor_cancelled
							// FIXME: this is somewhat expensive to do every mutation.
							for (auto& it : feedData->storageData) {
								if (refresh.canBeSet() && rep.mutations[resultLoc].version > it->desired.get()) {
									it->desired.set(rep.mutations[resultLoc].version);
								}
							}
						} else {
							ASSERT(rep.mutations[resultLoc].mutations.empty());
						}
						resultLoc++;
					}

					// if we got the empty version that went backwards, don't decrease nextVersion
					if (rep.mutations.back().version + 1 > nextVersion) {
						nextVersion = rep.mutations.back().version + 1;
					}

					if (refresh.canBeSet() && !atLatestVersion && rep.atLatestVersion) {
						atLatestVersion = true;
						feedData->notAtLatest.set(feedData->notAtLatest.get() - 1);
						if (feedData->notAtLatest.get() == 0 && feedData->context) {
							feedData->context->notAtLatestChangeFeeds.erase(feedData->dbgid);
						}
					}
					if (refresh.canBeSet() && rep.minStreamVersion > storageData->version.get()) {
						storageData->version.set(rep.minStreamVersion);
					}
					if (DEBUG_CF_CLIENT_TRACE) {
						TraceEvent(SevDebug, "TraceChangeFeedClientMergeCursorReplyDone", debugUID)
						    .detail("AtLatestNow", atLatestVersion);
					}
				}
				when(wait(atLatestVersion && replyStream.isEmpty() && results.isEmpty()
				              ? storageData->version.whenAtLeast(nextVersion)
				              : Future<Void>(Never()))) {
					MutationsAndVersionRef empty;
					empty.version = storageData->version.get();
					results.send(empty);
					nextVersion = storageData->version.get() + 1;
					if (DEBUG_CF_CLIENT_TRACE) {
						TraceEvent(SevDebug, "TraceChangeFeedClientMergeCursorSendEmpty", debugUID)
						    .detail("Version", empty.version);
					}
					lastEmpty = empty.version;
				}
				when(wait(atLatestVersion && replyStream.isEmpty() && !results.isEmpty() ? results.onEmpty()
				                                                                         : Future<Void>(Never()))) {}
			}
		}
	} catch (Error& e) {
		if (DEBUG_CF_CLIENT_TRACE) {
			TraceEvent(SevDebug, "TraceChangeFeedClientMergeCursorError", debugUID).errorUnsuppressed(e);
		}
		if (e.code() == error_code_actor_cancelled) {
			throw;
		}
		results.sendError(e);
		return Void();
	}
}

	void writeMutationsToCache(Reference<ChangeFeedCacheData> cacheData,
                           Reference<DatabaseContext> db,
                           Standalone<VectorRef<MutationsAndVersionRef>> cacheOut,
                           Key rangeID,
                           KeyRange range,
                           Key tenantPrefix) {
	if (!cacheData) {
		return;
	}
	ASSERT(cacheData->active);
	while (!cacheOut.empty() && cacheOut.front().version <= cacheData->latest) {
		cacheOut.pop_front();
	}
	if (!cacheOut.empty()) {
		Key durableKey = changeFeedCacheKey(tenantPrefix, rangeID, range, cacheOut.back().version);
		Value durableValue = changeFeedCacheValue(cacheOut);
		db->storage->set(KeyValueRef(durableKey, durableValue));
		cacheData->latest = cacheOut.back().version;
		db->uncommittedCFBytes += durableKey.size() + durableValue.size();
		if (db->uncommittedCFBytes > CLIENT_KNOBS->CHANGE_FEED_CACHE_FLUSH_BYTES) {
			db->commitChangeFeedStorage->set(true);
		}
	}
}

ACTOR Future<Void> mergeChangeFeedStreamInternal(Reference<ChangeFeedData> results,
                                                 Key rangeID,
                                                 KeyRange range,
                                                 std::vector<std::pair<StorageServerInterface, KeyRange>> interfs,
                                                 std::vector<MutationAndVersionStream> streams,
                                                 Version* begin,
                                                 Version end,
                                                 UID mergeCursorUID,
                                                 Reference<DatabaseContext> db,
                                                 Reference<ChangeFeedCacheData> cacheData,
                                                 Key tenantPrefix) {
	state Promise<Void> refresh = results->refresh;
	// with empty version handling in the partial cursor, all streams will always have a next element with version >=
	// the minimum version of any stream's next element
	state std::priority_queue<MutationAndVersionStream, std::vector<MutationAndVersionStream>> mutations;

	if (DEBUG_CF_CLIENT_TRACE) {
		TraceEvent(SevDebug, "TraceChangeFeedClientMergeCursorStart", mergeCursorUID)
		    .detail("StreamCount", interfs.size())
		    .detail("Begin", *begin)
		    .detail("End", end);
	}

	// previous version of change feed may have put a mutation in the promise stream and then immediately died. Wait for
	// that mutation first, so the promise stream always starts empty
	wait(results->mutations.onEmpty());
	wait(delay(0));
	ASSERT(results->mutations.isEmpty());

	if (DEBUG_CF_CLIENT_TRACE) {
		TraceEvent(SevDebug, "TraceChangeFeedClientMergeCursorGotEmpty", mergeCursorUID);
	}

	// update lastReturned once the previous mutation has been consumed
	if (*begin - 1 > results->lastReturnedVersion.get()) {
		results->lastReturnedVersion.set(*begin - 1);
	}

	state int interfNum = 0;

	state std::vector<MutationAndVersionStream> streamsUsed;
	// initially, pull from all streams
	for (auto& stream : streams) {
		streamsUsed.push_back(stream);
	}

	state Version nextVersion;
	loop {
		// bring all of the streams up to date to ensure we have the latest element from each stream in mutations
		interfNum = 0;
		while (interfNum < streamsUsed.size()) {
			try {
				Standalone<MutationsAndVersionRef> res = waitNext(streamsUsed[interfNum].results.getFuture());
				streamsUsed[interfNum].next = res;
				mutations.push(streamsUsed[interfNum]);
			} catch (Error& e) {
				if (e.code() != error_code_end_of_stream) {
					throw e;
				}
			}
			interfNum++;
		}

		if (mutations.empty()) {
			throw end_of_stream();
		}

		streamsUsed.clear();

		// Without this delay, weird issues with the last stream getting on another stream's callstack can happen
		wait(delay(0));

		// pop first item off queue - this will be mutation with the lowest version
		Standalone<VectorRef<MutationsAndVersionRef>> nextOut;
		nextVersion = mutations.top().next.version;

		streamsUsed.push_back(mutations.top());
		nextOut.push_back_deep(nextOut.arena(), mutations.top().next);
		mutations.pop();

		// for each other stream that has mutations with the same version, add it to nextOut
		while (!mutations.empty() && mutations.top().next.version == nextVersion) {
			if (mutations.top().next.mutations.size() &&
			    mutations.top().next.mutations.front().param1 != lastEpochEndPrivateKey) {
				nextOut.back().mutations.append_deep(
				    nextOut.arena(), mutations.top().next.mutations.begin(), mutations.top().next.mutations.size());
			}
			streamsUsed.push_back(mutations.top());
			mutations.pop();
		}

		ASSERT(nextOut.size() == 1);
		ASSERT(nextVersion >= *begin);

		*begin = nextVersion + 1;

		if (DEBUG_CF_CLIENT_TRACE) {
			TraceEvent(SevDebug, "TraceChangeFeedClientMergeCursorSending", mergeCursorUID)
			    .detail("Count", streamsUsed.size())
			    .detail("Version", nextVersion);
		}

		// send mutations at nextVersion to the client
		if (nextOut.back().mutations.empty()) {
			ASSERT(results->mutations.isEmpty());
		} else {
			ASSERT(nextOut.back().version > results->lastReturnedVersion.get());
			writeMutationsToCache(cacheData, db, nextOut, rangeID, range, tenantPrefix);
			results->mutations.send(nextOut);
			wait(results->mutations.onEmpty());
			wait(delay(0));
		}

		if (nextVersion > results->lastReturnedVersion.get()) {
			results->lastReturnedVersion.set(nextVersion);
		}
	}
}

ACTOR Future<Void> mergeChangeFeedStream(Reference<DatabaseContext> db,
                                         std::vector<std::pair<StorageServerInterface, KeyRange>> interfs,
                                         Reference<ChangeFeedData> results,
                                         Key rangeID,
                                         KeyRange range,
                                         Version* begin,
                                         Version end,
                                         int replyBufferSize,
                                         bool canReadPopped,
                                         ReadOptions readOptions,
                                         bool encrypted,
                                         Reference<ChangeFeedCacheData> cacheData,
                                         Key tenantPrefix) {
	state std::vector<Future<Void>> fetchers(interfs.size());
	state std::vector<Future<Void>> onErrors(interfs.size());
	state std::vector<MutationAndVersionStream> streams(interfs.size());
	state std::vector<Optional<ChangeFeedTSSValidationData>> tssDatas;
	tssDatas.reserve(interfs.size());
	for (int i = 0; i < interfs.size(); i++) {
		tssDatas.push_back({});
	}

	CODE_PROBE(interfs.size() > 10, "Large change feed merge cursor");
	CODE_PROBE(interfs.size() > 100, "Very large change feed merge cursor");

	state UID mergeCursorUID = UID();
	state std::vector<UID> debugUIDs;
	results->streams.clear();
	for (int i = 0; i < interfs.size(); i++) {
		ChangeFeedStreamRequest req;
		req.rangeID = rangeID;
		req.begin = *begin;
		req.end = end;
		req.range = interfs[i].second;
		req.canReadPopped = canReadPopped;
		// divide total buffer size among sub-streams, but keep individual streams large enough to be efficient
		req.replyBufferSize = replyBufferSize / interfs.size();
		if (replyBufferSize != -1 && req.replyBufferSize < CLIENT_KNOBS->CHANGE_FEED_STREAM_MIN_BYTES) {
			req.replyBufferSize = CLIENT_KNOBS->CHANGE_FEED_STREAM_MIN_BYTES;
		}
		req.options = readOptions;
		req.id = deterministicRandom()->randomUniqueID();
		req.encrypted = encrypted;

		debugUIDs.push_back(req.id);
		mergeCursorUID = UID(mergeCursorUID.first() ^ req.id.first(), mergeCursorUID.second() ^ req.id.second());

		results->streams.push_back(interfs[i].first.changeFeedStream.getReplyStream(req));
		maybeDuplicateTSSChangeFeedStream(req,
		                                  interfs[i].first.changeFeedStream,
		                                  db->enableLocalityLoadBalance ? &db->queueModel : nullptr,
		                                  &tssDatas[i]);
	}

	results->maxSeenVersion = invalidVersion;
	results->storageData.clear();
	Promise<Void> refresh = results->refresh;
	results->refresh = Promise<Void>();
	for (int i = 0; i < interfs.size(); i++) {
		results->storageData.push_back(db->getStorageData(interfs[i].first));
	}
	results->notAtLatest.set(interfs.size());
	if (results->context) {
		results->context->notAtLatestChangeFeeds[results->dbgid] = results.getPtr();
		results->created = now();
	}
	refresh.send(Void());

	for (int i = 0; i < interfs.size(); i++) {
		if (DEBUG_CF_CLIENT_TRACE) {
			TraceEvent(SevDebug, "TraceChangeFeedClientMergeCursorInit", debugUIDs[i])
			    .detail("CursorDebugUID", mergeCursorUID)
			    .detail("Idx", i)
			    .detail("FeedID", rangeID)
			    .detail("MergeRange", KeyRangeRef(interfs.front().second.begin, interfs.back().second.end))
			    .detail("PartialRange", interfs[i].second)
			    .detail("Begin", *begin)
			    .detail("End", end)
			    .detail("CanReadPopped", canReadPopped);
		}
		onErrors[i] = results->streams[i].onError();
		fetchers[i] = partialChangeFeedStream(interfs[i].first,
		                                      streams[i].results,
		                                      results->streams[i],
		                                      *begin,
		                                      end,
		                                      results,
		                                      results->storageData[i],
		                                      debugUIDs[i],
		                                      &tssDatas[i]);
	}

	wait(waitForAny(onErrors) ||
	     mergeChangeFeedStreamInternal(
	         results, rangeID, range, interfs, streams, begin, end, mergeCursorUID, db, cacheData, tenantPrefix));

	return Void();
}

ACTOR Future<KeyRange> getChangeFeedRange(Reference<DatabaseContext> db, Database cx, Key rangeID, Version begin = 0) {
	state Transaction tr(cx);
	state Key rangeIDKey = rangeID.withPrefix(changeFeedPrefix);

	auto cacheLoc = db->changeFeedCache.find(rangeID);
	if (cacheLoc != db->changeFeedCache.end()) {
		return cacheLoc->second;
	}

	loop {
		try {
			tr.setOption(FDBTransactionOptions::READ_SYSTEM_KEYS);
			tr.setOption(FDBTransactionOptions::PRIORITY_SYSTEM_IMMEDIATE);
			tr.setOption(FDBTransactionOptions::LOCK_AWARE);
			Version readVer = wait(tr.getReadVersion());
			if (readVer < begin) {
				wait(delay(FLOW_KNOBS->PREVENT_FAST_SPIN_DELAY));
				tr.reset();
			} else {
				Optional<Value> val = wait(tr.get(rangeIDKey));
				if (!val.present()) {
					ASSERT(tr.getReadVersion().isReady());
					TraceEvent(SevDebug, "ChangeFeedNotRegisteredGet")
					    .detail("FeedID", rangeID)
					    .detail("FullFeedKey", rangeIDKey)
					    .detail("BeginVersion", begin)
					    .detail("ReadVersion", tr.getReadVersion().get());
					throw change_feed_not_registered();
				}
				if (db->changeFeedCache.size() > CLIENT_KNOBS->CHANGE_FEED_CACHE_SIZE) {
					db->changeFeedCache.clear();
				}
				KeyRange range = std::get<0>(decodeChangeFeedValue(val.get()));
				db->changeFeedCache[rangeID] = range;
				return range;
			}
		} catch (Error& e) {
			wait(tr.onError(e));
		}
	}
}

ACTOR Future<Void> singleChangeFeedStreamInternal(KeyRange range,
                                                  Reference<ChangeFeedData> results,
                                                  Key rangeID,
                                                  Version* begin,
                                                  Version end,
                                                  Optional<ChangeFeedTSSValidationData>* tssData,
                                                  Reference<DatabaseContext> db,
                                                  Reference<ChangeFeedCacheData> cacheData,
                                                  Key tenantPrefix) {

	state Promise<Void> refresh = results->refresh;
	ASSERT(results->streams.size() == 1);
	ASSERT(results->storageData.size() == 1);
	state bool atLatest = false;

	// wait for any previous mutations in stream to be consumed
	wait(results->mutations.onEmpty());
	wait(delay(0));
	ASSERT(results->mutations.isEmpty());
	// update lastReturned once the previous mutation has been consumed
	if (*begin - 1 > results->lastReturnedVersion.get()) {
		results->lastReturnedVersion.set(*begin - 1);
		if (!refresh.canBeSet()) {
			try {
				// refresh is set if and only if this actor is cancelled
				wait(Future<Void>(Void()));
				// Catch any unexpected behavior if the above contract is broken
				ASSERT(false);
			} catch (Error& e) {
				ASSERT(e.code() == error_code_actor_cancelled);
				throw;
			}
		}
	}

	loop {
		ASSERT(refresh.canBeSet());
		state ChangeFeedStreamReply feedReply = waitNext(results->streams[0].getFuture());
		*begin = feedReply.mutations.back().version + 1;

		if (feedReply.popVersion > results->popVersion) {
			results->popVersion = feedReply.popVersion;
		}
		if (tssData->present()) {
			tssData->get().updatePopped(feedReply.popVersion);
		}

		// don't send completely empty set of mutations to promise stream
		bool anyMutations = false;
		for (auto& it : feedReply.mutations) {
			if (!it.mutations.empty()) {
				anyMutations = true;
				break;
			}
		}
		if (anyMutations) {
			// empty versions can come out of order, as we sometimes send explicit empty versions when restarting a
			// stream. Anything with mutations should be strictly greater than lastReturnedVersion
			ASSERT(feedReply.mutations.front().version > results->lastReturnedVersion.get());

			if (tssData->present()) {
				tssData->get().send(feedReply);
			}

			writeMutationsToCache(cacheData, db, feedReply.mutations, rangeID, range, tenantPrefix);
			results->mutations.send(
			    Standalone<VectorRef<MutationsAndVersionRef>>(feedReply.mutations, feedReply.arena));

			// Because onEmpty returns here before the consuming process, we must do a delay(0)
			wait(results->mutations.onEmpty());
			wait(delay(0));
		}

		// check refresh.canBeSet so that, if we are killed after calling one of these callbacks, we just
		// skip to the next wait and get actor_cancelled
		if (feedReply.mutations.back().version > results->lastReturnedVersion.get()) {
			results->lastReturnedVersion.set(feedReply.mutations.back().version);
		}

		if (!refresh.canBeSet()) {
			try {
				// refresh is set if and only if this actor is cancelled
				wait(Future<Void>(Void()));
				// Catch any unexpected behavior if the above contract is broken
				ASSERT(false);
			} catch (Error& e) {
				ASSERT(e.code() == error_code_actor_cancelled);
				throw;
			}
		}

		if (!atLatest && feedReply.atLatestVersion) {
			atLatest = true;
			results->notAtLatest.set(0);
			if (results->context) {
				results->context->notAtLatestChangeFeeds.erase(results->dbgid);
			}
		}

		if (feedReply.minStreamVersion > results->storageData[0]->version.get()) {
			results->storageData[0]->version.set(feedReply.minStreamVersion);
		}
	}
}

ACTOR Future<Void> singleChangeFeedStream(Reference<DatabaseContext> db,
                                          StorageServerInterface interf,
                                          KeyRange range,
                                          Reference<ChangeFeedData> results,
                                          Key rangeID,
                                          Version* begin,
                                          Version end,
                                          int replyBufferSize,
                                          bool canReadPopped,
                                          ReadOptions readOptions,
                                          bool encrypted,
                                          Reference<ChangeFeedCacheData> cacheData,
                                          Key tenantPrefix) {
	state Database cx(db);
	state ChangeFeedStreamRequest req;
	state Optional<ChangeFeedTSSValidationData> tssData;
	req.rangeID = rangeID;
	req.begin = *begin;
	req.end = end;
	req.range = range;
	req.canReadPopped = canReadPopped;
	req.replyBufferSize = replyBufferSize;
	req.options = readOptions;
	req.id = deterministicRandom()->randomUniqueID();
	req.encrypted = encrypted;

	if (DEBUG_CF_CLIENT_TRACE) {
		TraceEvent(SevDebug, "TraceChangeFeedClientSingleCursor", req.id)
		    .detail("FeedID", rangeID)
		    .detail("Range", range)
		    .detail("Begin", *begin)
		    .detail("End", end)
		    .detail("CanReadPopped", canReadPopped);
	}

	results->streams.clear();

	results->streams.push_back(interf.changeFeedStream.getReplyStream(req));

	results->maxSeenVersion = invalidVersion;
	results->storageData.clear();
	results->storageData.push_back(db->getStorageData(interf));
	Promise<Void> refresh = results->refresh;
	results->refresh = Promise<Void>();
	results->notAtLatest.set(1);
	if (results->context) {
		results->context->notAtLatestChangeFeeds[results->dbgid] = results.getPtr();
		results->created = now();
	}
	refresh.send(Void());

	maybeDuplicateTSSChangeFeedStream(
	    req, interf.changeFeedStream, cx->enableLocalityLoadBalance ? &cx->queueModel : nullptr, &tssData);

	wait(results->streams[0].onError() ||
	     singleChangeFeedStreamInternal(range, results, rangeID, begin, end, &tssData, db, cacheData, tenantPrefix));

	return Void();
}

void coalesceChangeFeedLocations(std::vector<KeyRangeLocationInfo>& locations) {
	// FIXME: only coalesce if same tenant!
	std::vector<UID> teamUIDs;
	bool anyToCoalesce = false;
	teamUIDs.reserve(locations.size());
	for (int i = 0; i < locations.size(); i++) {
		ASSERT(locations[i].locations->size() > 0);
		UID teamUID = locations[i].locations->getId(0);
		for (int j = 1; j < locations[i].locations->size(); j++) {
			UID locUID = locations[i].locations->getId(j);
			teamUID = UID(teamUID.first() ^ locUID.first(), teamUID.second() ^ locUID.second());
		}
		if (!teamUIDs.empty() && teamUIDs.back() == teamUID) {
			anyToCoalesce = true;
		}
		teamUIDs.push_back(teamUID);
	}

	if (!anyToCoalesce) {
		return;
	}

	CODE_PROBE(true, "coalescing change feed locations");

	// FIXME: there's technically a probability of "hash" collisions here, but it's extremely low. Could validate that
	// two teams with the same xor are in fact the same, or fall back to not doing this if it gets a wrong shard server
	// error or something

	std::vector<KeyRangeLocationInfo> coalesced;
	coalesced.reserve(locations.size());
	coalesced.push_back(locations[0]);
	for (int i = 1; i < locations.size(); i++) {
		if (teamUIDs[i] == teamUIDs[i - 1]) {
			coalesced.back().range = KeyRangeRef(coalesced.back().range.begin, locations[i].range.end);
		} else {
			coalesced.push_back(locations[i]);
		}
	}

	locations = coalesced;
}

ACTOR Future<bool> getChangeFeedStreamFromDisk(Reference<DatabaseContext> db,
                                               Reference<ChangeFeedData> results,
                                               Key rangeID,
                                               Version* begin,
                                               Version end,
                                               KeyRange range,
                                               Key tenantPrefix) {
	state bool foundEnd = false;
	loop {
		Key beginKey = changeFeedCacheKey(tenantPrefix, rangeID, range, *begin);
		Key endKey = changeFeedCacheKey(tenantPrefix, rangeID, range, MAX_VERSION);
		state RangeResult res = wait(db->storage->readRange(KeyRangeRef(beginKey, endKey),
		                                                    CLIENT_KNOBS->CHANGE_FEED_CACHE_LIMIT_BYTES,
		                                                    CLIENT_KNOBS->CHANGE_FEED_CACHE_LIMIT_BYTES));
		state int idx = 0;

		while (!foundEnd && idx < res.size()) {
			Standalone<VectorRef<MutationsAndVersionRef>> mutations = decodeChangeFeedCacheValue(res[idx].value);
			while (!mutations.empty() && mutations.front().version < *begin) {
				mutations.pop_front();
			}
			while (!mutations.empty() && mutations.back().version >= end) {
				mutations.pop_back();
				foundEnd = true;
			}
			if (!mutations.empty()) {
				*begin = mutations.back().version;
				results->mutations.send(mutations);
				wait(results->mutations.onEmpty());
				wait(delay(0));
				if (*begin > results->lastReturnedVersion.get()) {
					results->lastReturnedVersion.set(*begin);
				}
			}
			(*begin)++;
			idx++;
		}

		if (foundEnd || !res.more) {
			return foundEnd;
		}
	}
}

ACTOR Future<std::vector<KeyRangeLocationInfo>> getKeyRangeLocations_internal(
    Database cx,
    TenantInfo tenant,
    KeyRange keys,
    int limit,
    Reverse reverse,
    SpanContext spanContext,
    Optional<UID> debugID,
    UseProvisionalProxies useProvisionalProxies,
    Version version);

#include "fdbclient/GetKeyRangeLocations.h"

ACTOR Future<Void> getChangeFeedStreamActor(Reference<DatabaseContext> db,
                                            Reference<ChangeFeedData> results,
                                            Key rangeID,
                                            Version begin,
                                            Version end,
                                            KeyRange range,
                                            int replyBufferSize,
                                            bool canReadPopped,
                                            ReadOptions readOptions,
                                            bool encrypted,
                                            Reference<ChangeFeedCacheData> cacheData,
                                            Key tenantPrefix) {
	state Database cx(db);
	state Span span("NAPI:GetChangeFeedStream"_loc);

	state double sleepWithBackoff = CLIENT_KNOBS->WRONG_SHARD_SERVER_DELAY;
	state Version lastBeginVersion = invalidVersion;

	loop {
		state KeyRange keys;
		try {
			lastBeginVersion = begin;
			KeyRange fullRange = wait(getChangeFeedRange(db, cx, rangeID, begin));
			keys = fullRange & range;
			state std::vector<KeyRangeLocationInfo> locations =
			    wait(getKeyRangeLocations(cx,
			                              TenantInfo(),
			                              keys,
			                              CLIENT_KNOBS->CHANGE_FEED_LOCATION_LIMIT,
			                              Reverse::False,
			                              &StorageServerInterface::changeFeedStream,
			                              span.context,
			                              Optional<UID>(),
			                              UseProvisionalProxies::False,
			                              latestVersion));

			if (locations.size() >= CLIENT_KNOBS->CHANGE_FEED_LOCATION_LIMIT) {
				ASSERT_WE_THINK(false);
				throw unknown_change_feed();
			}

			if (CLIENT_KNOBS->CHANGE_FEED_COALESCE_LOCATIONS && locations.size() > 1) {
				coalesceChangeFeedLocations(locations);
			}

			state std::vector<int> chosenLocations(locations.size());
			state int loc = 0;
			while (loc < locations.size()) {
				// FIXME: create a load balance function for this code so future users of reply streams do not have
				// to duplicate this code
				int count = 0;
				int useIdx = -1;
				for (int i = 0; i < locations[loc].locations->size(); i++) {
					if (!IFailureMonitor::failureMonitor()
					         .getState(locations[loc]
					                       .locations->get(i, &StorageServerInterface::changeFeedStream)
					                       .getEndpoint())
					         .failed) {
						if (deterministicRandom()->random01() <= 1.0 / ++count) {
							useIdx = i;
						}
					}
				}

				if (useIdx >= 0) {
					chosenLocations[loc] = useIdx;
					loc++;
					if (g_network->isSimulated() && !g_simulator->speedUpSimulation && BUGGIFY_WITH_PROB(0.01)) {
						// simulate as if we had to wait for all alternatives delayed, before the next one
						wait(delay(deterministicRandom()->random01()));
					}
					continue;
				}

				std::vector<Future<Void>> ok(locations[loc].locations->size());
				for (int i = 0; i < ok.size(); i++) {
					ok[i] = IFailureMonitor::failureMonitor().onStateEqual(
					    locations[loc].locations->get(i, &StorageServerInterface::changeFeedStream).getEndpoint(),
					    FailureStatus(false));
				}

				// Making this SevWarn means a lot of clutter
				if (now() - g_network->networkInfo.newestAlternativesFailure > 1 ||
				    deterministicRandom()->random01() < 0.01) {
					TraceEvent("AllAlternativesFailed").detail("Alternatives", locations[0].locations->description());
				}

				wait(allAlternativesFailedDelay(quorum(ok, 1)));
				loc = 0;
			}

			++db->feedStreamStarts;

			if (locations.size() > 1) {
				++db->feedMergeStreamStarts;
				std::vector<std::pair<StorageServerInterface, KeyRange>> interfs;
				for (int i = 0; i < locations.size(); i++) {
					interfs.emplace_back(locations[i].locations->getInterface(chosenLocations[i]),
					                     locations[i].range & range);
				}
				CODE_PROBE(true, "Change feed merge cursor");
				// TODO (jslocum): validate connectionFileChanged behavior
				wait(mergeChangeFeedStream(db,
				                           interfs,
				                           results,
				                           rangeID,
				                           range,
				                           &begin,
				                           end,
				                           replyBufferSize,
				                           canReadPopped,
				                           readOptions,
				                           encrypted,
				                           cacheData,
				                           tenantPrefix) ||
				     cx->connectionFileChanged());
			} else {
				CODE_PROBE(true, "Change feed single cursor");
				StorageServerInterface interf = locations[0].locations->getInterface(chosenLocations[0]);
				wait(singleChangeFeedStream(db,
				                            interf,
				                            range,
				                            results,
				                            rangeID,
				                            &begin,
				                            end,
				                            replyBufferSize,
				                            canReadPopped,
				                            readOptions,
				                            encrypted,
				                            cacheData,
				                            tenantPrefix) ||
				     cx->connectionFileChanged());
			}
		} catch (Error& e) {
			if (e.code() == error_code_actor_cancelled || e.code() == error_code_change_feed_popped) {
				results->streams.clear();
				results->storageData.clear();
				if (e.code() == error_code_change_feed_popped) {
					++db->feedNonRetriableErrors;
					CODE_PROBE(true, "getChangeFeedStreamActor got popped", probe::decoration::rare);
					results->mutations.sendError(e);
					results->refresh.sendError(e);
				} else {
					results->refresh.sendError(change_feed_cancelled());
				}
				throw;
			}
			if (results->notAtLatest.get() == 0) {
				results->notAtLatest.set(1);
				if (results->context) {
					results->context->notAtLatestChangeFeeds[results->dbgid] = results.getPtr();
					results->created = now();
				}
			}

			if (e.code() == error_code_wrong_shard_server || e.code() == error_code_all_alternatives_failed ||
			    e.code() == error_code_connection_failed || e.code() == error_code_unknown_change_feed ||
			    e.code() == error_code_broken_promise || e.code() == error_code_future_version ||
			    e.code() == error_code_request_maybe_delivered ||
			    e.code() == error_code_storage_too_many_feed_streams) {
				++db->feedErrors;
				db->changeFeedCache.erase(rangeID);
				cx->invalidateCache({}, keys);
				if (begin == lastBeginVersion || e.code() == error_code_storage_too_many_feed_streams) {
					// We didn't read anything since the last failure before failing again.
					// Back off quickly and exponentially, up to 1 second
					sleepWithBackoff = std::min(2.0, sleepWithBackoff * 5);
					sleepWithBackoff = std::max(0.1, sleepWithBackoff);
				} else {
					sleepWithBackoff = CLIENT_KNOBS->WRONG_SHARD_SERVER_DELAY;
				}
				TraceEvent("ChangeFeedClientError")
				    .errorUnsuppressed(e)
				    .suppressFor(30.0)
				    .detail("FeedID", rangeID)
				    .detail("BeginVersion", begin)
				    .detail("AnyProgress", begin != lastBeginVersion);
				wait(delay(sleepWithBackoff));
			} else {
				if (e.code() != error_code_end_of_stream) {
					++db->feedNonRetriableErrors;
					TraceEvent("ChangeFeedClientErrorNonRetryable").errorUnsuppressed(e).suppressFor(5.0);
				}
				results->mutations.sendError(e);
				results->refresh.sendError(change_feed_cancelled());
				results->streams.clear();
				results->storageData.clear();
				return Void();
			}
		}
	}
}

ACTOR Future<Void> durableChangeFeedMonitor(Reference<DatabaseContext> db,
                                            Reference<ChangeFeedData> results,
                                            Key rangeID,
                                            Version begin,
                                            Version end,
                                            KeyRange range,
                                            int replyBufferSize,
                                            bool canReadPopped,
                                            ReadOptions readOptions,
                                            bool encrypted,
                                            Future<Key> tenantPrefix) {
	state Optional<ChangeFeedCacheRange> cacheRange;
	state Reference<ChangeFeedCacheData> data;
	state Error err = success();
	state Version originalBegin = begin;
	results->endVersion = end;
	db->usedAnyChangeFeeds = true;
	try {
		if (db->storage != nullptr) {
			wait(db->initializeChangeFeedCache);
			Key prefix = wait(tenantPrefix);
			cacheRange = ChangeFeedCacheRange(prefix, rangeID, range);
			if (db->changeFeedCaches.count(cacheRange.get())) {
				auto cacheData = db->changeFeedCaches[cacheRange.get()];
				if (begin < cacheData->popped) {
					results->mutations.sendError(change_feed_popped());
					return Void();
				}
				if (cacheData->version <= begin) {
					bool foundEnd = wait(getChangeFeedStreamFromDisk(db, results, rangeID, &begin, end, range, prefix));
					if (foundEnd) {
						results->mutations.sendError(end_of_stream());
						return Void();
					}
				}
			}
			if (end == MAX_VERSION) {
				if (!db->changeFeedCaches.count(cacheRange.get())) {
					data = makeReference<ChangeFeedCacheData>();
					data->version = begin;
					data->active = true;
					db->changeFeedCaches[cacheRange.get()] = data;
					db->rangeId_cacheData[cacheRange.get().rangeId][cacheRange.get()] = data;
					Key durableFeedKey = changeFeedCacheFeedKey(cacheRange.get().tenantPrefix, rangeID, range);
					Value durableFeedValue = changeFeedCacheFeedValue(begin, 0);
					db->storage->set(KeyValueRef(durableFeedKey, durableFeedValue));
				} else {
					data = db->changeFeedCaches[cacheRange.get()];
					if (!data->active && data->version <= begin) {
						data->active = true;
						if (originalBegin > data->latest + 1) {
							data->version = originalBegin;
							Key durableFeedKey = changeFeedCacheFeedKey(cacheRange.get().tenantPrefix, rangeID, range);
							Value durableFeedValue = changeFeedCacheFeedValue(originalBegin, data->popped);
							db->storage->set(KeyValueRef(durableFeedKey, durableFeedValue));
						}
					} else {
						data = Reference<ChangeFeedCacheData>();
					}
				}
			}
		}
		wait(getChangeFeedStreamActor(db,
		                              results,
		                              rangeID,
		                              begin,
		                              end,
		                              range,
		                              replyBufferSize,
		                              canReadPopped,
		                              readOptions,
		                              encrypted,
		                              data,
		                              cacheRange.present() ? cacheRange.get().tenantPrefix : Key()));
	} catch (Error& e) {
		err = e;
	}
	if (data) {
		data->active = false;
		data->inactiveTime = now();
	}
	if (err.code() != error_code_success) {
		throw err;
	}
	return Void();
}

Future<Void> DatabaseContext::getChangeFeedStream(Reference<ChangeFeedData> results,
                                                  Key rangeID,
                                                  Version begin,
                                                  Version end,
                                                  KeyRange range,
                                                  int replyBufferSize,
                                                  bool canReadPopped,
                                                  ReadOptions readOptions,
                                                  bool encrypted,
                                                  Future<Key> tenantPrefix) {
	return durableChangeFeedMonitor(Reference<DatabaseContext>::addRef(this),
	                                results,
	                                rangeID,
	                                begin,
	                                end,
	                                range,
	                                replyBufferSize,
	                                canReadPopped,
	                                readOptions,
	                                encrypted,
	                                tenantPrefix);
}

Version OverlappingChangeFeedsInfo::getFeedMetadataVersion(const KeyRangeRef& range) const {
	Version v = invalidVersion;
	for (auto& it : feedMetadataVersions) {
		if (it.second > v && it.first.intersects(range)) {
			v = it.second;
		}
	}
	return v;
}

ACTOR Future<OverlappingChangeFeedsReply> singleLocationOverlappingChangeFeeds(Database cx,
                                                                               Reference<LocationInfo> location,
                                                                               KeyRangeRef range,
                                                                               Version minVersion) {
	state OverlappingChangeFeedsRequest req;
	req.range = range;
	req.minVersion = minVersion;

	OverlappingChangeFeedsReply rep = wait(loadBalance(cx.getPtr(),
	                                                   location,
	                                                   &StorageServerInterface::overlappingChangeFeeds,
	                                                   req,
	                                                   TaskPriority::DefaultPromiseEndpoint,
	                                                   AtMostOnce::False,
	                                                   cx->enableLocalityLoadBalance ? &cx->queueModel : nullptr));
	return rep;
}

bool compareChangeFeedResult(const OverlappingChangeFeedEntry& i, const OverlappingChangeFeedEntry& j) {
	return i.feedId < j.feedId;
}

ACTOR Future<OverlappingChangeFeedsInfo> getOverlappingChangeFeedsActor(Reference<DatabaseContext> db,
                                                                        KeyRangeRef range,
                                                                        Version minVersion) {
	state Database cx(db);
	state Span span("NAPI:GetOverlappingChangeFeeds"_loc);

	loop {
		try {
			state std::vector<KeyRangeLocationInfo> locations =
			    wait(getKeyRangeLocations(cx,
			                              TenantInfo(),
			                              range,
			                              CLIENT_KNOBS->CHANGE_FEED_LOCATION_LIMIT,
			                              Reverse::False,
			                              &StorageServerInterface::overlappingChangeFeeds,
			                              span.context,
			                              Optional<UID>(),
			                              UseProvisionalProxies::False,
			                              latestVersion));

			if (locations.size() >= CLIENT_KNOBS->CHANGE_FEED_LOCATION_LIMIT) {
				TraceEvent(SevError, "OverlappingRangeTooLarge")
				    .detail("Range", range)
				    .detail("Limit", CLIENT_KNOBS->CHANGE_FEED_LOCATION_LIMIT);
				wait(delay(1.0));
				throw all_alternatives_failed();
			}

			state std::vector<Future<OverlappingChangeFeedsReply>> allOverlappingRequests;
			for (auto& it : locations) {
				allOverlappingRequests.push_back(
				    singleLocationOverlappingChangeFeeds(cx, it.locations, it.range & range, minVersion));
			}
			wait(waitForAll(allOverlappingRequests));

			OverlappingChangeFeedsInfo result;
			std::unordered_map<KeyRef, OverlappingChangeFeedEntry> latestFeedMetadata;
			for (int i = 0; i < locations.size(); i++) {
				result.arena.dependsOn(allOverlappingRequests[i].get().arena);
				result.arena.dependsOn(locations[i].range.arena());
				result.feedMetadataVersions.push_back(
				    { locations[i].range, allOverlappingRequests[i].get().feedMetadataVersion });
				for (auto& it : allOverlappingRequests[i].get().feeds) {
					auto res = latestFeedMetadata.insert({ it.feedId, it });
					if (!res.second) {
						CODE_PROBE(true, "deduping fetched overlapping feed by higher metadata version");
						if (res.first->second.feedMetadataVersion < it.feedMetadataVersion) {
							res.first->second = it;
						}
					}
				}
			}
			for (auto& it : latestFeedMetadata) {
				result.feeds.push_back(result.arena, it.second);
			}
			return result;
		} catch (Error& e) {
			if (e.code() == error_code_wrong_shard_server || e.code() == error_code_all_alternatives_failed ||
			    e.code() == error_code_future_version) {
				cx->invalidateCache({}, range);
				wait(delay(CLIENT_KNOBS->WRONG_SHARD_SERVER_DELAY));
			} else {
				throw e;
			}
		}
	}
}

Future<OverlappingChangeFeedsInfo> DatabaseContext::getOverlappingChangeFeeds(KeyRangeRef range, Version minVersion) {
	return getOverlappingChangeFeedsActor(Reference<DatabaseContext>::addRef(this), range, minVersion);
}

ACTOR static Future<Void> popChangeFeedBackup(Database cx, Key rangeID, Version version) {
	++cx->feedPopsFallback;
	state Transaction tr(cx);
	loop {
		try {
			tr.setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
			tr.setOption(FDBTransactionOptions::PRIORITY_SYSTEM_IMMEDIATE);
			tr.setOption(FDBTransactionOptions::LOCK_AWARE);
			state Key rangeIDKey = rangeID.withPrefix(changeFeedPrefix);
			Optional<Value> val = wait(tr.get(rangeIDKey));
			if (val.present()) {
				KeyRange range;
				Version popVersion;
				ChangeFeedStatus status;
				std::tie(range, popVersion, status) = decodeChangeFeedValue(val.get());
				if (version > popVersion) {
					tr.set(rangeIDKey, changeFeedValue(range, version, status));
				}
			} else {
				ASSERT(tr.getReadVersion().isReady());
				TraceEvent(SevDebug, "ChangeFeedNotRegisteredPop")
				    .detail("FeedID", rangeID)
				    .detail("FullFeedKey", rangeIDKey)
				    .detail("PopVersion", version)
				    .detail("ReadVersion", tr.getReadVersion().get());
				throw change_feed_not_registered();
			}
			wait(tr.commit());
			return Void();
		} catch (Error& e) {
			wait(tr.onError(e));
		}
	}
}

ACTOR Future<Void> popChangeFeedMutationsActor(Reference<DatabaseContext> db, Key rangeID, Version version) {
	state Database cx(db);
	state Key rangeIDKey = rangeID.withPrefix(changeFeedPrefix);
	state Span span("NAPI:PopChangeFeedMutations"_loc);
	db->usedAnyChangeFeeds = true;
	++db->feedPops;

	if (db->rangeId_cacheData.count(rangeID)) {
		auto& feeds = db->rangeId_cacheData[rangeID];
		for (auto& it : feeds) {
			if (version > it.second->popped) {
				it.second->popped = version;
				Key beginKey = changeFeedCacheKey(it.first.tenantPrefix, it.first.rangeId, it.first.range, 0);
				Key endKey = changeFeedCacheKey(it.first.tenantPrefix, it.first.rangeId, it.first.range, version);
				db->storage->clear(KeyRangeRef(beginKey, endKey));
				Key durableFeedKey = changeFeedCacheFeedKey(it.first.tenantPrefix, it.first.rangeId, it.first.range);
				Value durableFeedValue = changeFeedCacheFeedValue(it.second->version, it.second->popped);
				db->storage->set(KeyValueRef(durableFeedKey, durableFeedValue));
				db->uncommittedCFBytes +=
				    beginKey.size() + endKey.size() + durableFeedKey.size() + durableFeedValue.size();
				if (db->uncommittedCFBytes > CLIENT_KNOBS->CHANGE_FEED_CACHE_FLUSH_BYTES) {
					db->commitChangeFeedStorage->set(true);
				}
			}
		}
	}

	state KeyRange keys = wait(getChangeFeedRange(db, cx, rangeID));

	state std::vector<KeyRangeLocationInfo> locations =
	    wait(getKeyRangeLocations(cx,
	                              TenantInfo(),
	                              keys,
	                              3,
	                              Reverse::False,
	                              &StorageServerInterface::changeFeedPop,
	                              span.context,
	                              Optional<UID>(),
	                              UseProvisionalProxies::False,
	                              latestVersion));

	if (locations.size() > 2) {
		wait(popChangeFeedBackup(cx, rangeID, version));
		return Void();
	}

	auto model = cx->enableLocalityLoadBalance ? &cx->queueModel : nullptr;

	bool foundFailed = false;
	for (int i = 0; i < locations.size() && !foundFailed; i++) {
		for (int j = 0; j < locations[i].locations->size() && !foundFailed; j++) {
			if (IFailureMonitor::failureMonitor()
			        .getState(locations[i].locations->get(j, &StorageServerInterface::changeFeedPop).getEndpoint())
			        .isFailed()) {
				foundFailed = true;
			}
			// for now, if any of popping SS has a TSS pair, just always use backup method
			if (model && model
			                 ->getTssData(locations[i]
			                                  .locations->get(j, &StorageServerInterface::changeFeedPop)
			                                  .getEndpoint()
			                                  .token.first())
			                 .present()) {
				foundFailed = true;
			}
		}
	}

	if (foundFailed) {
		wait(popChangeFeedBackup(cx, rangeID, version));
		return Void();
	}

	try {
		// FIXME: lookup both the src and dest shards as of the pop version to ensure all locations are popped
		std::vector<Future<Void>> popRequests;
		for (int i = 0; i < locations.size(); i++) {
			for (int j = 0; j < locations[i].locations->size(); j++) {
				popRequests.push_back(locations[i].locations->getInterface(j).changeFeedPop.getReply(
				    ChangeFeedPopRequest(rangeID, version, locations[i].range)));
			}
		}
		choose {
			when(wait(waitForAll(popRequests))) {}
			when(wait(delay(CLIENT_KNOBS->CHANGE_FEED_POP_TIMEOUT))) {
				wait(popChangeFeedBackup(cx, rangeID, version));
			}
		}
	} catch (Error& e) {
		if (e.code() != error_code_unknown_change_feed && e.code() != error_code_wrong_shard_server &&
		    e.code() != error_code_all_alternatives_failed && e.code() != error_code_broken_promise &&
		    e.code() != error_code_server_overloaded) {
			throw;
		}
		db->changeFeedCache.erase(rangeID);
		cx->invalidateCache({}, keys);
		wait(popChangeFeedBackup(cx, rangeID, version));
	}
	return Void();
}

Future<Void> DatabaseContext::popChangeFeedMutations(Key rangeID, Version version) {
	return popChangeFeedMutationsActor(Reference<DatabaseContext>::addRef(this), rangeID, version);
}

