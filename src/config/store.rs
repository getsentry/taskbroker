use anyhow::Result;
use serde::{Deserialize, Serialize};
use tracing::warn;

use crate::config::Config;
use crate::store::adapters::postgres;

#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash, Deserialize, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum DatabaseAdapter {
    /// SQLite database adapter
    Sqlite,

    /// PostgreSQL database adapter
    Postgres,
}

impl DatabaseAdapter {
    pub async fn migrate(&self, config: &Config) -> Result<()> {
        match self {
            Self::Postgres => postgres::migrate(config).await,
            Self::Sqlite => {
                warn!("Standalone migration not supported for SQLite");
                Ok(())
            }
        }
    }
}
