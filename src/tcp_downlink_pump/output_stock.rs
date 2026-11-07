//! Original FIFO adapters around one sealed physical backing ledger.
//! The ledger owns the entire slab/loan transaction; adapters own only original
//! queue calls and notification. This boundary restricts Q capabilities, adding
//! no runtime forwarding or DATA authority. Entry: cold queue factory/producer.
//! Lifecycle tests live beside the ledger under ledger/tests.
mod ledger;
mod notify;
pub(super) use notify::QueueReturn;
mod queue;
pub(crate) use ledger::{OutputStock, StockProducer, OutputSpace, StockWaitError, StockKey};
pub(super) use ledger::{Root, Loan, StockLayout};

pub(super) use queue::ReadAdmission;
pub(crate) use queue::StockReadOperation;
