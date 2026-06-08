use serde::{Deserialize, Serialize};

#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash, Deserialize, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum DatabaseAdapter {
    /// SQLite database adapter
    Sqlite,

    /// PostgreSQL database adapter
    Postgres,
}
