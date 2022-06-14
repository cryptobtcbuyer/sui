
use std::path::Path;
use crate::default_db_options;

use sui_types::{
    base_types::{AuthorityName, TransactionDigest},
    error::{SuiError, SuiResult},
    messages::CertifiedTransaction,
};

use typed_store::rocks::DBMap;
use typed_store::{reopen, traits::Map};

/// Pending store is used by nodes to store downloaded objects (certs, etc) that have not yet been
/// applied to the node's SuiDataStore.
pub struct PendingStore {

    /// Certificates that have been fetched from remote validators, but not sequenced.
    certificates: DBMap<TransactionDigest, CertifiedTransaction>,
}

impl PendingStore {

    pub fn open<P: AsRef<Path>>(path: P) -> Result<Self, SuiError> {
        let (options, _) = default_db_options(None);

        let db = {
            let path = &path;
            let db_options = Some(options.clone());
            let opt_cfs: &[(&str, &rocksdb::Options)] = &[("certificates", &options)];
            typed_store::rocks::open_cf_opts(path, db_options, opt_cfs)
        }
        .map_err(SuiError::StorageError)?;

        let certificates = reopen!(&db, "certificates";<TransactionDigest, CertifiedTransaction>);

        Ok(Self { certificates })
    }

    pub fn has_cert(&self, tx: &TransactionDigest) -> SuiResult<bool> {
        Ok(self.certificates.contains_key(tx)?)
    }

    pub fn store_cert(&self, tx: &TransactionDigest, cert: &CertifiedTransaction) -> SuiResult {
        Ok(self.certificates.insert(tx, cert)?)
    }

    pub fn get_cert(&self, tx: &TransactionDigest) -> SuiResult<Option<CertifiedTransaction>> {
        Ok(self.certificates.get(tx)?)
    }
}
