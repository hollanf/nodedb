#[global_allocator]
static ALLOC: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use tracing::info;
use tracing_subscriber::EnvFilter;

use synapsedb::ServerConfig;
use synapsedb::bridge::dispatch::Dispatcher;
use synapsedb::control::state::SharedState;
use synapsedb::data::executor::core_loop::CoreLoop;
use synapsedb::wal::WalManager;

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
        "starting synapsedb"
    );

    // Validate engine config.
    config.engines.validate()?;

    // Initialize memory governor.
    let byte_budgets = config.engines.to_byte_budgets(config.memory_limit);
    let _governor = synapsedb::memory::init_governor(config.memory_limit, &byte_budgets)?;

    // Open WAL.
    let wal = Arc::new(WalManager::open(&config.wal_dir(), false)?);
    info!(next_lsn = %wal.next_lsn(), "WAL ready");

    // Create SPSC bridge: Dispatcher (Control Plane) + CoreChannelDataSide (Data Plane).
    let num_cores = config.data_plane_cores;
    let (dispatcher, data_sides) = Dispatcher::new(num_cores, 1024);

    // Create shared state.
    let shared = SharedState::new(dispatcher, Arc::clone(&wal));

    // Start Data Plane cores on dedicated OS threads (thread-per-core).
    let mut core_handles = Vec::with_capacity(num_cores);
    for (core_id, data_side) in data_sides.into_iter().enumerate() {
        let data_dir = config.data_dir.clone();
        let handle = std::thread::Builder::new()
            .name(format!("data-core-{core_id}"))
            .spawn(move || {
                let mut core = CoreLoop::open(
                    core_id,
                    data_side.request_rx,
                    data_side.response_tx,
                    &data_dir,
                )
                .expect("failed to open CoreLoop engines");
                info!(core_id, "data plane core started");

                // Event loop: drain SPSC, execute tasks, yield when idle.
                loop {
                    let processed = core.tick();
                    if processed == 0 {
                        // No work — yield briefly to avoid busy-spinning.
                        std::thread::sleep(Duration::from_micros(50));
                    }
                }
            })?;
        core_handles.push(handle);
    }

    info!(num_cores, "data plane cores running");

    // Start response poller: routes Data Plane responses to waiting sessions.
    let shared_poller = Arc::clone(&shared);
    tokio::spawn(async move {
        loop {
            shared_poller.poll_and_route_responses();
            tokio::time::sleep(Duration::from_micros(100)).await;
        }
    });

    let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);

    let listener = synapsedb::control::server::listener::Listener::bind(config.listen).await?;

    // Handle Ctrl+C.
    tokio::spawn(async move {
        tokio::signal::ctrl_c().await.ok();
        info!("shutdown signal received");
        let _ = shutdown_tx.send(true);
    });

    listener.run(shared, shutdown_rx).await?;

    Ok(())
}
