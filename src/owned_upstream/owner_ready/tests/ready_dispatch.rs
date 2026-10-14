//! Metadata recipe routing for the outer root dispatch scenarios.
//! Owns no root, command fixture or test body; recipes remain in private children.

mod publisher_fatal;

use std::sync::Arc;
use std::task::Wake;
