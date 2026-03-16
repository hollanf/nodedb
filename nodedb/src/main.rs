#[global_allocator]
static ALLOC: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use tracing::info;
use tracing_subscriber::EnvFilter;

use nodedb::ServerConfig;
use nodedb::bridge::dispatch::Dispatcher;
use nodedb::control::state::SharedState;
use nodedb::data::runtime::spawn_core;
use nodedb::wal::WalManager;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Initialize tracing.
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env())
        .init();

    // Load config.
    let config = match std::env::args().nth(1) {
        Some(path) => ServerConfig::from_file(&PathBuf::from(path))?,
        None => {
            info!("no config file provided, using defaults");
            ServerConfig::default()
        }
    };

    info!(
        listen = %config.listen,
        cores = config.data_plane_cores,
        memory_limit = config.memory_limit,
        "starting nodedb"
    );

    // Validate engine config.
    config.engines.validate()?;

    // Initialize memory governor.
    let byte_budgets = config.engines.to_byte_budgets(config.memory_limit);
    let _governor = nodedb::memory::init_governor(config.memory_limit, &byte_budgets)?;

    // Open WAL.
    let wal = Arc::new(WalManager::open(&config.wal_dir(), false)?);
    info!(next_lsn = %wal.next_lsn(), "WAL ready");

    // Create SPSC bridge: Dispatcher (Control Plane) + CoreChannelDataSide (Data Plane).
    let num_cores = config.data_plane_cores;
    let (mut dispatcher, data_sides) = Dispatcher::new(num_cores, 1024);

    // Start Data Plane cores on dedicated OS threads (thread-per-core).
    // Each core gets: jemalloc arena pinning + eventfd-driven wake.
    let mut core_handles = Vec::with_capacity(num_cores);
    let mut notifiers = Vec::with_capacity(num_cores);
    for (core_id, data_side) in data_sides.into_iter().enumerate() {
        let (handle, notifier) = spawn_core(
            core_id,
            data_side.request_rx,
            data_side.response_tx,
            &config.data_dir,
        )?;
        core_handles.push(handle);
        notifiers.push((core_id, notifier));
    }

    // Wire notifiers into the dispatcher so it signals cores after pushing requests.
    for (core_id, notifier) in &notifiers {
        dispatcher.set_notifier(*core_id, *notifier);
    }

    info!(num_cores, "data plane cores running (eventfd-driven)");

    // Create shared state (must happen after notifiers are wired).
    let shared = SharedState::new(dispatcher, Arc::clone(&wal));

    // Start response poller: routes Data Plane responses to waiting sessions.
    let shared_poller = Arc::clone(&shared);
    tokio::spawn(async move {
        loop {
            shared_poller.poll_and_route_responses();
            tokio::time::sleep(Duration::from_micros(100)).await;
        }
    });

    let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);

    let listener = nodedb::control::server::listener::Listener::bind(config.listen).await?;

    // Handle Ctrl+C.
    tokio::spawn(async move {
        tokio::signal::ctrl_c().await.ok();
        info!("shutdown signal received");
        let _ = shutdown_tx.send(true);
    });

    listener.run(shared, shutdown_rx).await?;

    Ok(())
}
