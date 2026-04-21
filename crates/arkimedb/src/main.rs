use std::path::PathBuf;
use std::sync::Arc;
use anyhow::Result;
use clap::{Parser, Subcommand};
use arkimedb_core::config::{Config, HttpConfig, StorageConfig};
use arkimedb_storage::Engine;
use arkimedb_http::{router, AppState};

#[derive(Parser)]
#[command(name = "arkimedb", version, about = "Single-node Arkime data store")]
struct Cli {
    #[command(subcommand)]
    cmd: Cmd,
}

#[derive(Subcommand)]
enum Cmd {
    /// Start the HTTP server.
    Serve {
        #[arg(long, default_value = "./data")]
        data_dir: PathBuf,
        #[arg(long, default_value = "0.0.0.0")]
        bind: String,
        #[arg(long, default_value_t = 9200)]
        port: u16,
        #[arg(long, default_value_t = 9)]
        zstd_level: i32,
        #[arg(long)]
        config: Option<PathBuf>,
    },
    /// Initialize an empty data directory and exit.
    Init {
        #[arg(long, default_value = "./data")]
        data_dir: PathBuf,
    },
    /// Print version.
    Version,
}

fn raise_fd_limit() {
    // Bump soft file-descriptor limit toward the hard limit. Default on macOS
    // is 256 which isn't enough for many collections * redb handles + sockets.
    #[cfg(unix)]
    unsafe {
        let mut rl: libc::rlimit = std::mem::zeroed();
        if libc::getrlimit(libc::RLIMIT_NOFILE, &mut rl) != 0 { return; }
        let old = rl.rlim_cur;
        // Target a large but sane value; macOS silently caps at
        // kern.maxfilesperproc (~10k-64k) so oversized requests fail.
        let desired: libc::rlim_t = 65536;
        let target = desired.min(rl.rlim_max);
        if target > rl.rlim_cur {
            rl.rlim_cur = target;
            if libc::setrlimit(libc::RLIMIT_NOFILE, &rl) != 0 {
                // Fall back to progressively smaller values.
                for v in [32768u64, 16384, 10240, 4096, 2048] {
                    rl.rlim_cur = (v as libc::rlim_t).min(rl.rlim_max);
                    if libc::setrlimit(libc::RLIMIT_NOFILE, &rl) == 0 { break; }
                }
            }
        }
        let mut now: libc::rlimit = std::mem::zeroed();
        let _ = libc::getrlimit(libc::RLIMIT_NOFILE, &mut now);
        if now.rlim_cur != old {
            eprintln!("[arkimedb] raised RLIMIT_NOFILE soft limit: {} -> {}", old, now.rlim_cur);
        }
    }
}

fn setup_tracing() {
    let filter = std::env::var("RUST_LOG").unwrap_or_else(|_| "info,tower_http=warn,hyper=warn".into());
    tracing_subscriber::fmt().with_env_filter(filter).compact().init();
}

#[tokio::main]
async fn main() -> Result<()> {
    raise_fd_limit();
    setup_tracing();
    let cli = Cli::parse();
    match cli.cmd {
        Cmd::Version => { println!("arkimedb {}", env!("CARGO_PKG_VERSION")); Ok(()) }
        Cmd::Init { data_dir } => {
            std::fs::create_dir_all(&data_dir)?;
            let _ = Engine::open(Config {
                data_dir, http: HttpConfig { bind: "0.0.0.0".into(), port: 9200 },
                storage: StorageConfig { zstd_level: 9, memtable_max_records: 50_000, memtable_max_bytes: 64*1024*1024 },
            })?;
            println!("initialized");
            Ok(())
        }
        Cmd::Serve { data_dir, bind, port, zstd_level, config } => {
            let cfg = if let Some(p) = config {
                let mut c = Config::load(&p)?;
                c.data_dir = data_dir;
                c.http.bind = bind;
                c.http.port = port;
                c.storage.zstd_level = zstd_level;
                c
            } else {
                Config {
                    data_dir,
                    http: HttpConfig { bind, port },
                    storage: StorageConfig { zstd_level, memtable_max_records: 50_000, memtable_max_bytes: 64*1024*1024 },
                }
            };
            let engine = Engine::open(cfg.clone())?;
            let state = Arc::new(AppState {
                engine,
                start: std::time::Instant::now(),
                scrolls: parking_lot::RwLock::new(ahash::AHashMap::new()),
                cluster_settings: parking_lot::RwLock::new(Default::default()),
            });
            arkimedb_http::init_http_log_from_env();
            let app = router(state.clone()).layer(tower_http::trace::TraceLayer::new_for_http());
            let addr: std::net::SocketAddr = format!("{}:{}", cfg.http.bind, cfg.http.port).parse()?;
            tracing::info!("arkimedb listening on http://{addr}");
            let listener = tokio::net::TcpListener::bind(addr).await?;
            axum::serve(listener, app).await?;
            Ok(())
        }
    }
}
