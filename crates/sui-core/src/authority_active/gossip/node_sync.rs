use super::{Follower, FollowerDigestHandler};
use crate::{
    authority::AuthorityState, authority_aggregator::AuthorityAggregator,
    authority_client::AuthorityAPI, safe_client::SafeClient,
};
use async_trait::async_trait;

use std::collections::{btree_map, hash_map, BTreeMap, HashMap};
use sui_storage::pending_store::PendingStore;
use sui_types::{
    base_types::{AuthorityName, ExecutionDigests, TransactionDigest, TransactionEffectsDigest},
    committee::{Committee, StakeUnit},
    error::{SuiError, SuiResult},
    messages::TransactionInfoRequest,
};

use std::ops::Deref;
use std::sync::Arc;

use tokio::sync::{
    broadcast,
    mpsc::{channel, Receiver, Sender},
    Mutex, Semaphore,
};
use tokio::task::JoinHandle;

use tracing::{error, info, trace, warn};

const NODE_SYNC_QUEUE_LEN: usize = 500;

// Sync up to 20 certificates concurrently.
const MAX_NODE_SYNC_CONCURRENCY: usize = 20;

/// EffectsStakeMap tracks which effects digests have been attested by a quorum of validators and
/// are thus final.
struct EffectsStakeMap {
    /// Keep track of how much stake has voted for a given effects digest
    /// any entry in this map with >2f+1 stake can be sequenced locally and
    /// removed from the map.
    effects_stake_map: HashMap<ExecutionDigests, StakeUnit>,
    /// Keep track of stake votes per validator - needed to double check the total stored in
    /// effects_stake_map, which can otherwise be corrupted by byzantine double-voting.
    effects_vote_map: HashMap<(ExecutionDigests, AuthorityName), StakeUnit>,
}

impl EffectsStakeMap {
    pub fn new() -> Self {
        Self {
            effects_stake_map: HashMap::new(),
            effects_vote_map: HashMap::new(),
        }
    }

    /// Note that a given effects digest has been attested by a validator, and return true if the
    /// stake that has attested that effects digest has exceeded the quorum threshold.
    pub fn note_effects_digest(
        &mut self,
        source: &AuthorityName,
        stake: StakeUnit,
        quorum_threshold: StakeUnit,
        digests: &ExecutionDigests,
    ) -> bool {
        let vote_entry = self.effects_vote_map.entry((*digests, *source));

        let cur_stake = if let hash_map::Entry::Occupied(_) = &vote_entry {
            // TODO: report byzantine authority suspciion
            warn!(peer = ?source, digest = ?digests.transaction, effects = ?digests.effects,
                "ByzantineAuthoritySuspicion: peer double-voted for effects digest");
            self.effects_stake_map.entry(*digests).or_insert(0)
        } else {
            vote_entry.or_insert(stake);

            self.effects_stake_map
                .entry(*digests)
                .and_modify(|cur| *cur += stake)
                .or_insert(stake)
        };

        *cur_stake > quorum_threshold
    }
}

struct DigestsMessage {
    digests: ExecutionDigests,
    peer: AuthorityName,
}

struct CertificateHandler {
    state: Arc<AuthorityState>,
}

#[async_trait]
impl ConfirmationTransactionHandler for LocalConfirmationTransactionHandler {
    async fn handle(&self, cert: ConfirmationTransaction) -> SuiResult<TransactionInfoResponse> {
        self.state.handle_confirmation_transaction(cert).await
    }

    fn destination_name(&self) -> String {
        format!("{:?}", self.state.name)
    }
}

struct NodeSyncState<A> {
    committee: Arc<Committee>,
    effects_stake: Mutex<EffectsStakeMap>,
    state: Arc<AuthorityState>,
    pending_store: Arc<PendingStore>,
    aggregator: Arc<AuthorityAggregator<A>>,
    pending_downloads: Mutex<HashMap<TransactionDigest, broadcast::Sender<SuiResult>>>,
}

impl<A> NodeSyncState<A>
where
    A: AuthorityAPI + Send + Sync + 'static + Clone,
{
    fn start(self_: Arc<Self>, mut receiver: Receiver<DigestsMessage>) -> JoinHandle<()> {
        tokio::spawn(async move {
            // this pattern for limiting concurrency is from
            // https://github.com/tokio-rs/tokio/discussions/2648
            let limit = Arc::new(Semaphore::new(MAX_NODE_SYNC_CONCURRENCY));

            while let Some(DigestsMessage { digests, peer }) = receiver.recv().await {
                let permit = Arc::clone(&limit).acquire_owned().await;
                let self_ = self_.clone();
                tokio::spawn(async move {
                    let _permit = permit; // hold semaphore permit until task completes
                    self_.process_digest(peer, digests).await;
                });
            }
        })
    }

    // TODO: This function should use AuthorityAggregator to try multiple validators - even though
    // we are fetching the cert from the same validator that sent us the tx digest, and even though
    // we know the cert is final, any given validator may be byzantine and refuse to give us thee
    // cert.
    async fn download_cert_impl(
        peer: AuthorityName,
        aggregator: Arc<AuthorityAggregator<A>>,
        digest: &TransactionDigest,
    ) -> SuiResult {

        /*
        aggregator.sync_authority_source_to_destination(
            ConfirmationTransaction { certificate },
            follower.peer_name,
            LocalConfirmationTransactionHandler {
                state: follower.state.clone(),
            },
        ).await?;
        */
        let client = aggregator.clone_client(&peer);

        let resp = client
            .handle_transaction_info_request(TransactionInfoRequest::from(*digest))
            .await
            .expect("TODO: need to use authority aggregator to download cert");

        let effects = resp.signed_effects.ok_or_else(|| {
            SuiError::ByzantineAuthoritySuspicion { authority: () }
        })?

        let cert = resp.certified_transaction.ok_or_else(|| {
            info!(?digest, ?peer, "validator did not return cert");
            SuiError::GenericAuthorityError {
                error: format!("validator did not return cert for {:?}", digest),
            }
        })?;

        // TODO: write cert to pending store.
        Ok(())
    }

    async fn download_cert(&self, peer: &AuthorityName, digest: &TransactionDigest) -> SuiResult {
        // TODO: check if its in the pending store

        let mut rx = {
            let pending_downloads = &mut self.pending_downloads.lock().await;
            let entry = pending_downloads.entry(*digest);

            match entry {
                hash_map::Entry::Occupied(e) => {
                    trace!(?digest, ?peer, "subscribing to pending request");
                    e.get().subscribe()
                }
                hash_map::Entry::Vacant(e) => {
                    trace!(?digest, ?peer, "request certificate");
                    let (tx, rx) = broadcast::channel(1);
                    e.insert(tx.clone());

                    let aggregator = self.aggregator.clone();

                    let digest = *digest;
                    let peer = *peer;
                    tokio::task::spawn(async move {
                        if let Err(error) =
                            tx.send(Self::download_cert_impl(peer, aggregator, &digest).await)
                        {
                            error!(?digest, ?peer, ?error, "Could not broadcast cert response");
                        }
                    });
                    rx
                }
            }
        };
        rx.recv()
            .await
            .map_err(|e| SuiError::GenericAuthorityError {
                error: format!("{:?}", e),
            })?
    }

    async fn process_digest(&self, peer: AuthorityName, digests: ExecutionDigests) -> SuiResult {
        // check if we the tx is already locally final
        if self.state.database.effects_exists(&digests.transaction)? {
            return Ok(());
        }

        // Check if the tx is final.
        let stake = self.committee.weight(&peer);
        let quorum_threshold = self.committee.quorum_threshold();
        let is_final = self.effects_stake.lock().await.note_effects_digest(
            &peer,
            stake,
            quorum_threshold,
            &digests
        );

        self.download_cert(&peer, &digests.transaction).await?;

        if is_final {
            let cert = self.pending_store.get_cert(digests.transaction);
            let resp = self.state.handle_confirmation_transaction(cert).await;
        }
        // load cert from pending store

        Ok(())
    }
}

#[derive(Clone)]
pub struct NodeSyncDigestHandler {
    _sync_join_handle: Arc<JoinHandle<()>>,
    sender: Sender<DigestsMessage>,
}

impl NodeSyncDigestHandler {
    pub fn new<A>(state: Arc<AuthorityState>, aggregator: Arc<AuthorityAggregator<A>>) -> Self
    where
        A: AuthorityAPI + Send + Sync + 'static + Clone,
    {
        let (sender, receiver) = channel(NODE_SYNC_QUEUE_LEN);

        let committee = state.committee.load().deref().clone();
        let sync_state = Arc::new(NodeSyncState {
            committee,
            effects_stake: Mutex::new(EffectsStakeMap::new()),
            state,
            aggregator,
            pending_downloads: Mutex::new(HashMap::new()),
        });

        let _sync_join_handle = Arc::new(NodeSyncState::start(sync_state, receiver));

        Self {
            _sync_join_handle,
            sender,
        }
    }
}

#[async_trait]
impl<A> FollowerDigestHandler<A> for NodeSyncDigestHandler
where
    A: AuthorityAPI + Send + Sync + 'static + Clone,
{
    async fn handle_digest(&self, follower: &Follower<A>, digests: ExecutionDigests) -> SuiResult {
        self.sender
            .send(DigestsMessage {
                digests,
                peer: follower.peer_name,
            })
            .await
            .map_err(|e| SuiError::GenericAuthorityError {
                error: e.to_string(),
            })
    }
}

#[cfg(test)]
mod tests {

    use sui_types::{base_types::AuthorityName, crypto::get_key_pair};

    fn random_authority_name() -> AuthorityName {
        let key = get_key_pair();
        *key.1.public_key_bytes()
    }

    #[test]
    fn test_effects_stake() {
        let mut map = EffectsStakeMap::new();

        let threshold = 3;

        let byzantine = random_authority_name();
        let validator2 = random_authority_name();
        let validator3 = random_authority_name();

        let digest = TransactionEffectsDigest::random();

        assert!(!map.note_effects_digest(byzantine, 1, threshold, digest));
        assert!(!map.note_effects_digest(validator2, 1, threshold, digest));

        // double voting is rejected
        assert!(!map.note_effects_digest(byzantine, 1, threshold, digest));

        // final vote pushes us over.
        assert!(map.note_effects_digest(validator3, 1, threshold, digest));

        // double vote doesn't result in false if we already exceeded threshold.
        assert!(map.note_effects_digest(byzantine, 1, threshold, digest));
    }
}
