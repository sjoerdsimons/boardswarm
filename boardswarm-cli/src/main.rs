use std::{
    cmp::Ordering,
    convert::Infallible,
    io::SeekFrom,
    path::{Path, PathBuf},
    time::Duration,
};

use anyhow::{anyhow, bail, Context};
use async_compression::futures::bufread::GzipDecoder;
use bmap_parser::Bmap;
use boardswarm_client::{
    client::{Boardswarm, BoardswarmBuilder, VolumeIoRW},
    config,
    device::{Device, DeviceVolume},
    oidc::{OidcClientBuilder, StdoutAuth},
};
use boardswarm_protocol::ItemType;
use bytes::{Bytes, BytesMut};
use clap::{arg, builder::PossibleValue, Args, Parser, Subcommand, ValueEnum};
use futures::{pin_mut, FutureExt, Stream, StreamExt, TryStreamExt};
use http::Uri;
use indicatif::ProgressBar;
use itertools::Itertools;
use rockfile::boot::{
    RkBootEntry, RkBootEntryBytes, RkBootHeader, RkBootHeaderBytes, RkBootHeaderEntry,
};
use tokio::{
    fs::File,
    io::{AsyncBufReadExt, AsyncReadExt, AsyncSeekExt, AsyncWriteExt, BufReader},
};

use boardswarm_client::client::ItemEvent;
use tokio_util::compat::{TokioAsyncReadCompatExt, TokioAsyncWriteCompatExt};
use tracing::{debug, info};
use ui::TerminalSizeSetting;
use utils::BatchWriter;

mod ui;
mod ui_term;
mod utils;

#[derive(Clone, Copy, Debug)]
struct ItemTypes(pub ItemType);

impl From<ItemTypes> for ItemType {
    fn from(val: ItemTypes) -> Self {
        val.0
    }
}

impl From<ItemType> for ItemTypes {
    fn from(val: ItemType) -> Self {
        Self(val)
    }
}

impl std::fmt::Display for ItemTypes {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if f.alternate() {
            std::fmt::Debug::fmt(&self.0, f)
        } else {
            match self.0 {
                ItemType::Device => f.write_str("device"),
                ItemType::Console => f.write_str("console"),
                ItemType::Actuator => f.write_str("actuator"),
                ItemType::Volume => f.write_str("volume"),
            }
        }
    }
}

impl ValueEnum for ItemTypes {
    fn value_variants<'a>() -> &'a [Self] {
        &[
            ItemTypes(ItemType::Actuator),
            ItemTypes(ItemType::Console),
            ItemTypes(ItemType::Device),
            ItemTypes(ItemType::Volume),
        ]
    }

    fn to_possible_value(&self) -> Option<PossibleValue> {
        Some(match self.0 {
            ItemType::Actuator => PossibleValue::new("actuators"),
            ItemType::Console => PossibleValue::new("consoles"),
            ItemType::Device => PossibleValue::new("devices"),
            ItemType::Volume => PossibleValue::new("volumes"),
        })
    }
}

fn find_bmap(img: &Path) -> Option<PathBuf> {
    fn append(path: PathBuf) -> PathBuf {
        let mut p = path.into_os_string();
        p.push(".bmap");
        p.into()
    }

    let mut bmap = img.to_path_buf();
    loop {
        bmap = append(bmap);
        if bmap.exists() {
            return Some(bmap);
        }
        // Drop .bmap
        bmap.set_extension("");
        bmap.extension()?;
        // Drop existing orignal extension part
        bmap.set_extension("");
    }
}

async fn write_aimg<W: AsyncWriteExt + AsyncSeekExt + Unpin>(
    mut io: W,
    path: &Path,
) -> anyhow::Result<()> {
    let mut f = tokio::fs::File::open(path).await?;
    let mut header_bytes = android_sparse_image::FileHeaderBytes::default();
    f.read_exact(&mut header_bytes).await?;
    let header = android_sparse_image::FileHeader::from_bytes(&header_bytes)
        .context("Parsing android sparse image header")?;

    println!("Writing android sparse image");
    let progress = ProgressBar::new(header.chunks.into());
    for i in 0..header.chunks {
        progress.set_position(i.into());
        let mut chunk_bytes = android_sparse_image::ChunkHeaderBytes::default();
        f.read_exact(&mut chunk_bytes).await?;
        let chunk = android_sparse_image::ChunkHeader::from_bytes(&chunk_bytes)
            .context("Parsing chunk header")?;
        let out_size = chunk.out_size(&header);
        match chunk.chunk_type {
            android_sparse_image::ChunkType::Raw => {
                let mut raw = (&mut f).take(out_size as u64);
                tokio::io::copy(&mut raw, &mut io).await?;
            }
            android_sparse_image::ChunkType::Fill => {
                let mut fill = [0u8; 4];
                f.read_exact(&mut fill).await?;
                for _ in 0..out_size / 4 {
                    io.write_all(&fill).await?;
                }
            }
            android_sparse_image::ChunkType::DontCare => {
                io.seek(SeekFrom::Current(out_size as i64)).await?;
            }
            android_sparse_image::ChunkType::Crc32 => {
                debug!("Ignoring sparse image crc32");
            }
        }
    }
    io.shutdown().await?;
    progress.finish();
    Ok(())
}

async fn write_bmap(io: VolumeIoRW, path: &Path) -> anyhow::Result<()> {
    let bmap_path = find_bmap(path).ok_or_else(|| anyhow!("Failed to find bmap"))?;
    println!("Using bmap file: {}", path.display());

    let mut bmap_file = tokio::fs::File::open(bmap_path).await?;
    let mut xml = String::new();
    bmap_file.read_to_string(&mut xml).await?;
    let bmap = Bmap::from_xml(&xml)?;

    let progress = ProgressBar::new(bmap.total_mapped_size());
    let mut batchwriter = utils::BatchWriter::new(io).discard_flush();
    let mut writer = progress.wrap_async_write(&mut batchwriter).compat_write();

    let file = tokio::fs::File::open(path).await?;
    match path.extension().and_then(std::ffi::OsStr::to_str) {
        Some("gz") => {
            let gz = GzipDecoder::new(BufReader::new(file).compat());
            let mut gz = bmap_parser::AsyncDiscarder::new(gz);
            bmap_parser::copy_async(&mut gz, &mut writer, &bmap).await?;
        }
        _ => {
            bmap_parser::copy_async(&mut file.compat(), &mut writer, &bmap).await?;
        }
    }
    writer.into_inner().shutdown().await?;
    progress.finish();

    Ok(())
}

async fn copy_output_to_stdout<O>(output: O) -> anyhow::Result<()>
where
    O: Stream<Item = Bytes>,
{
    pin_mut!(output);
    let mut stdout = tokio::io::stdout();
    while let Some(data) = output.next().await {
        stdout.write_all(&data).await?;
        stdout.flush().await?;
    }
    Ok(())
}

fn input_stream() -> impl Stream<Item = Bytes> {
    let stdin = tokio::io::stdin();

    let mut stdin_termios = nix::sys::termios::tcgetattr(&stdin)
        .context("tcgetattr failed")
        .unwrap();

    nix::sys::termios::cfmakeraw(&mut stdin_termios);
    nix::sys::termios::tcsetattr(&stdin, nix::sys::termios::SetArg::TCSANOW, &stdin_termios)
        .context("tcsetattr failed")
        .unwrap();

    futures::stream::unfold(stdin, |mut stdin| async move {
        let mut data = BytesMut::zeroed(64);
        let r = stdin.read(&mut data).await.ok()?;
        data.truncate(r);
        Some((data.into(), stdin))
    })
}

async fn rock_download_entry(
    header: RkBootHeaderEntry,
    target: &str,
    file: &mut File,
    volume: &mut DeviceVolume,
) -> anyhow::Result<()> {
    for i in 0..header.count {
        let mut entry: RkBootEntryBytes = [0; 57];

        file.seek(SeekFrom::Start(
            header.offset as u64 + (header.size * i) as u64,
        ))
        .await?;
        file.read_exact(&mut entry).await?;

        let entry = RkBootEntry::from_bytes(&entry);
        println!("{} Name: {}", i, String::from_utf16(entry.name.as_slice())?);

        let mut data = vec![0; entry.data_size as usize];
        file.seek(SeekFrom::Start(entry.data_offset as u64)).await?;
        file.read_exact(&mut data).await?;

        let mut target = volume.open(target, Some(data.len() as u64)).await?;
        target.write_all(&data).await?;
        target.shutdown().await?;

        println!("Done!... waiting {}ms", entry.data_delay);
        if entry.data_delay > 0 {
            tokio::time::sleep(Duration::from_millis(entry.data_delay as u64)).await;
        }
    }

    Ok(())
}

async fn rock_download_boot(volume: &mut DeviceVolume, path: &Path) -> anyhow::Result<()> {
    let mut file = File::open(path).await?;
    let mut header: RkBootHeaderBytes = [0; 102];
    file.read_exact(&mut header).await?;
    let header =
        RkBootHeader::from_bytes(&header).ok_or_else(|| anyhow!("Failed to parse header"))?;

    rock_download_entry(header.entry_471, "471", &mut file, volume).await?;
    rock_download_entry(header.entry_472, "472", &mut file, volume).await?;
    Ok(())
}

#[derive(Debug, clap::Parser)]
struct AuthInitArg {
    /// Configure the url
    #[clap(short, long)]
    uri: Uri,
    /// Configure the JWT auth token
    #[clap(short, long)]
    token: Option<String>,
    /// Read new JWT auth token from the given file
    #[clap(long, conflicts_with = "token")]
    token_file: Option<PathBuf>,
}

#[derive(Debug, clap::Parser)]
struct AuthModifyArg {
    /// Configure the url
    #[clap(short, long)]
    uri: Option<Uri>,
    /// Configure the JWT auth token
    #[clap(short, long)]
    token: Option<String>,
    /// Read new JWT auth token from the given file
    #[clap(long, conflicts_with = "token")]
    token_file: Option<PathBuf>,
}

#[derive(Debug, Subcommand)]
enum AuthCommand {
    /// Initialize new authentication to a server
    Init(AuthInitArg),
    /// Output information about a single authentication
    Info,
    /// List all configured authentications
    List,
    /// Set default authentication
    SetDefault,
    /// Remove an authentication to a server
    Remove,
    /// Modify an authentication to a server
    Modify(AuthModifyArg),
}

#[derive(Clone, Debug)]
enum ItemArg {
    Id(u64),
    Name(String),
}

async fn item_lookup<I: Into<ItemTypes>>(
    arg: ItemArg,
    item_type: I,
    mut client: Boardswarm,
) -> Result<u64, anyhow::Error> {
    let item_type: ItemTypes = item_type.into();
    match arg {
        ItemArg::Id(id) => Ok(id),
        ItemArg::Name(name) => {
            let mut items = client.list(item_type.into()).await?;

            let (name, instance) = name
                .rsplit_once('@')
                .map_or((name.as_str(), None), |(n, i)| (n, Some(i)));

            items.retain(|i| i.name == name && i.instance.as_deref() == instance);

            match items.len() {
                0 => bail!("{item_type:#} not found"),
                1 => Ok(items[0].id),
                /* The items are uniquely identified only with their ids so this could happen */
                _ => bail!("Duplicate {item_type} name {name}"),
            }
        }
    }
}

fn parse_actuator(device: &str) -> Result<ItemArg, Infallible> {
    if let Ok(id) = device.parse() {
        Ok(ItemArg::Id(id))
    } else {
        Ok(ItemArg::Name(device.to_string()))
    }
}

#[derive(Debug, Args)]
struct ActuatorMode {
    /// Actuator specific mode in json format
    mode: String,
}

#[derive(Debug, Subcommand)]
enum ActuatorCommand {
    /// Change actuator mode
    ChangeMode(ActuatorMode),
    /// Display actuator properties
    Properties,
}

fn parse_console(device: &str) -> Result<ItemArg, Infallible> {
    if let Ok(id) = device.parse() {
        Ok(ItemArg::Id(id))
    } else {
        Ok(ItemArg::Name(device.to_string()))
    }
}

#[derive(Debug, Args)]
struct ConsoleConfigure {
    /// Console specific configure in json format
    configuration: String,
}

#[derive(Debug, Subcommand)]
enum ConsoleCommand {
    /// Configure a console
    Configure(ConsoleConfigure),
    /// Tail the output of a device console
    Tail,
    /// Connect input and output to a device console
    Connect,
    /// Display console properties
    Properties,
}

#[derive(Debug, Args)]
struct WriteArgs {
    /// Offset in bytes to write to
    #[clap(short, long)]
    offset: Option<u64>,
    /// Target to write to
    target: String,
    /// File to write
    file: PathBuf,
}

#[derive(Debug, Args)]
struct AimgWriteArgs {
    /// Target to write the bmap file to
    target: String,
    /// Path to bmap file
    file: PathBuf,
}

#[derive(Debug, Args)]
struct BmapWriteArgs {
    /// Target to write the bmap file to
    target: String,
    /// Path to bmap file
    file: PathBuf,
}

fn parse_volume(device: &str) -> Result<ItemArg, Infallible> {
    if let Ok(id) = device.parse() {
        Ok(ItemArg::Id(id))
    } else {
        Ok(ItemArg::Name(device.to_string()))
    }
}

#[derive(Debug, Subcommand)]
enum VolumeCommand {
    /// Retrieve volume information
    Info,
    /// Upload file to the volume
    Write(WriteArgs),
    /// Write a bmap file to the volume
    WriteBmap(BmapWriteArgs),
    /// Write a android sparse image to the volume
    WriteAimg(AimgWriteArgs),
    /// Commit upload
    Commit,
    /// Commit upload
    Erase { target: String },
    /// Display volume properties
    Properties,
}

#[derive(Clone, Debug)]
enum DeviceArg {
    Id(u64),
    Name(String),
}

impl DeviceArg {
    async fn device(
        &self,
        client: Boardswarm,
    ) -> Result<Option<boardswarm_client::device::Device>, anyhow::Error> {
        let builder = boardswarm_client::device::DeviceBuilder::from_client(client);
        match self {
            DeviceArg::Id(id) => Ok(Some(builder.by_id(*id).await?)),
            DeviceArg::Name(name) => Ok(builder.by_name(name).await?),
        }
    }
}

fn parse_device(device: &str) -> Result<DeviceArg, Infallible> {
    if let Ok(id) = device.parse() {
        Ok(DeviceArg::Id(id))
    } else {
        Ok(DeviceArg::Name(device.to_string()))
    }
}

#[derive(Clone, Debug, Args)]
struct DeviceConsoleArgs {
    /// Console to open instead of the default
    #[clap(short, long)]
    console: Option<String>,
}

#[derive(Debug, Args)]
struct DeviceModeArgs {
    /// Mode to change the device to
    mode: String,
}

#[derive(Debug, Args)]
struct DeviceCommonVolumeArgs {
    /// Wait for the volume to appear
    #[arg(short, long)]
    wait: bool,
    /// The volume
    volume: String,
}

impl DeviceCommonVolumeArgs {
    async fn open(&self, device: &Device) -> anyhow::Result<DeviceVolume> {
        let volume = device
            .volume_by_name(&self.volume)
            .ok_or_else(|| anyhow!("Volume not available for device"))?;
        if !volume.available() {
            if self.wait {
                println!("Waiting for volume..");
                volume.wait().await;
            } else {
                bail!("volume not available");
            }
        }
        Ok(volume)
    }
}

#[derive(Debug, Args)]
struct DeviceCommonVolumeTargetArgs {
    #[clap(flatten)]
    volume: DeviceCommonVolumeArgs,
    /// The volume target
    target: String,
}

impl DeviceCommonVolumeTargetArgs {
    async fn open(&self, device: &Device) -> anyhow::Result<(DeviceVolume, VolumeIoRW)> {
        self.do_open(device, None).await
    }

    async fn open_with_len(
        &self,
        device: &Device,
        len: u64,
    ) -> anyhow::Result<(DeviceVolume, VolumeIoRW)> {
        self.do_open(device, Some(len)).await
    }

    async fn do_open(
        &self,
        device: &Device,
        len: Option<u64>,
    ) -> anyhow::Result<(DeviceVolume, VolumeIoRW)> {
        let mut volume = self.volume.open(device).await?;
        let rw = volume.open(&self.target, len).await?;
        Ok((volume, rw))
    }
}

#[derive(Debug, Args)]
struct DeviceReadArg {
    /// Offset in bytes for reading to start
    #[arg(short, long)]
    offset: Option<u64>,
    /// Amount of bytes to be read
    #[arg(short, long)]
    length: Option<u64>,
    #[clap(flatten)]
    target: DeviceCommonVolumeTargetArgs,
    /// Path to the file to write the read data to
    file: PathBuf,
}

#[derive(Debug, Args)]
struct DeviceEraseArg {
    #[clap(flatten)]
    volume: DeviceCommonVolumeArgs,
    /// The volume target to erase
    target: String,
}

#[derive(Debug, Args)]
struct DeviceWriteArg {
    /// Write at the given offset rather then from the start
    #[arg(short, long)]
    offset: Option<u64>,
    /// Commit the volume after finishing the write
    #[arg(short, long)]
    commit: bool,
    #[clap(flatten)]
    target: DeviceCommonVolumeTargetArgs,
    /// Path to the file to write
    file: PathBuf,
}

#[derive(Debug, Args)]
struct DeviceAimgWriteArg {
    /// Commit the volume after finishing the write
    #[arg(short, long)]
    commit: bool,
    #[clap(flatten)]
    target: DeviceCommonVolumeTargetArgs,
    /// Path to the bmap file to write
    file: PathBuf,
}

#[derive(Debug, Args)]
struct DeviceBmapWriteArg {
    /// Commit the volume after finishing the write
    #[arg(short, long)]
    commit: bool,
    #[clap(flatten)]
    target: DeviceCommonVolumeTargetArgs,
    /// Path to the bmap file to write
    file: PathBuf,
}

#[derive(Debug, Subcommand)]
enum DeviceCommand {
    /// Get info about a device
    Info {
        /// Monitor changes to the device information
        #[arg(short, long)]
        follow: bool,
    },
    /// Read data from a device volume
    Read(DeviceReadArg),
    /// Write data to a device volume
    Write(DeviceWriteArg),
    /// Write a android sparse image file to a device volume
    WriteAimg(DeviceAimgWriteArg),
    /// Write a bmap file to a device volume
    WriteBmap(DeviceBmapWriteArg),
    /// Erase a target from a volume
    Erase(DeviceEraseArg),
    /// Commit a volume
    Commit(DeviceCommonVolumeArgs),
    /// Change device mode
    Mode(DeviceModeArgs),
    /// Turn the device off and on again
    Reset,
    /// Connect to the console
    Connect(DeviceConsoleArgs),
    /// Tail to the console
    Tail(DeviceConsoleArgs),
    /// Display device properties
    Properties,
}

#[derive(Debug, Subcommand)]
enum RockCommand {
    /// Transfer a combined boot file containing images of type 0x471 and 0x472 to a rock device
    DownloadBoot {
        #[arg(short, long)]
        wait: bool,
        #[clap(short, long)]
        volume: Option<String>,
        path: PathBuf,
    },
}

#[derive(Debug, Subcommand)]
enum Command {
    /// Configure client authentication to boardswarm servers
    Auth {
        #[command(subcommand)]
        command: AuthCommand,
    },
    /// Retrieve the login information from the remote boardswarm server
    LoginInfo,
    /// Actuator specific commands
    Actuator {
        /// The actuator to use
        #[arg(value_parser = parse_actuator)]
        actuator: ItemArg,
        #[command(subcommand)]
        command: ActuatorCommand,
    },
    /// Console specific commands
    Console {
        /// The console to use
        #[arg(value_parser = parse_console)]
        console: ItemArg,
        #[command(subcommand)]
        command: ConsoleCommand,
    },
    /// Volumes specific commands
    Volume {
        /// The volume to use
        #[arg(value_parser = parse_volume)]
        volume: ItemArg,
        #[command(subcommand)]
        command: VolumeCommand,
    },
    /// Device specific commands
    Device {
        #[arg(value_parser = parse_device)]
        /// The device to use
        device: DeviceArg,
        #[command(subcommand)]
        command: DeviceCommand,
    },
    /// Commands specific to rockchip devices
    Rock {
        #[arg(value_parser = parse_device)]
        /// The device to use
        device: DeviceArg,
        #[command(subcommand)]
        command: RockCommand,
    },
    /// List all items of a given type
    List {
        #[arg(value_enum)]
        /// The type of items to list
        type_: ItemTypes,
        #[clap(long, short)]
        verbose: bool,
    },
    /// Monitor registered items of a given type
    Monitor {
        #[arg(value_enum)]
        /// The type of items to monitor
        type_: ItemTypes,
        #[clap(long, short)]
        verbose: bool,
    },
    /// Open the UI for a given device
    Ui {
        #[arg(value_parser = parse_device)]
        /// The device to use
        device: DeviceArg,
        #[command(flatten)]
        console: DeviceConsoleArgs,
        /// Terminal size, with the possible formats:
        ///- "<columns>x<lines>" (ex: "72x18") where the terminal size is fixed
        ///- "auto", where it is autoresized to match the window size
        #[clap(long, default_value = "80x24", verbatim_doc_comment)]
        terminal_size: TerminalSizeSetting,
        #[clap(long, default_value_t = 5000)]
        /// Number of lines to keep for scrollback
        scrollback_lines: usize,
    },
}

async fn run_auth_init(
    mut config: config::Config,
    instance: &str,
    init_args: AuthInitArg,
) -> anyhow::Result<()> {
    if let Some(s) = config.find_server(instance) {
        bail!(
            "Authentication for {} already exists, use modify instead.",
            s.name
        );
    }

    let config_path = config.path().to_owned();

    let auth = if let Some(token) = &init_args.token {
        info!("Using authentication token provided: {:?}", token);
        config::Auth::Token(token.clone())
    } else if let Some(ref token_path) = init_args.token_file {
        info!(
            "Importing authentication token from the path provided: {:?}",
            token_path
        );
        let file = tokio::fs::File::open(token_path).await?;
        let mut reader = BufReader::new(file);
        let mut token = String::new();
        let read = reader.read_line(&mut token).await?;
        // Arbitrary minimal size to make sure might actually be a token without going all the way
        // and parsing it. Mostly to avoid accidentally using an empty file
        if read < 16 {
            bail!("Token file doesn't look like a JWT token (too small)");
        }
        let token = token.trim_end();
        config::Auth::Token(token.to_string())
    } else {
        // OIDC
        info!("Using OIDC authentication");
        let mut boardswarm = BoardswarmBuilder::new(init_args.uri.clone())
            .connect()
            .await?;
        let info = boardswarm.login_info().await?;

        // TODO allow user select the !first one
        let login = info
            .first()
            .context("No OIDC authentication to choose from")?;
        println!("Starting login with {}", login.description);
        match &login.method {
            boardswarm_client::client::AuthMethod::Oidc { url, client_id } => {
                let mut token_file = instance.replace([std::path::MAIN_SEPARATOR, '.'], "_");
                token_file.push_str(".token");
                let token_cache = config_path.with_file_name(token_file);

                let mut builder = OidcClientBuilder::new(url.parse()?, client_id);
                builder.token_cache(token_cache.clone());
                builder.login_provider(StdoutAuth());

                let mut oidc = builder.build();
                oidc.auth().await?;
                config::Auth::Oidc {
                    uri: url.parse().unwrap(),
                    client_id: client_id.clone(),
                    token_cache,
                }
            }
        }
    };

    let new = config::Server {
        name: instance.to_owned(),
        uri: init_args.uri.clone(),
        auth,
    };
    config.add_server(new);

    config.write().await?;

    Ok(())
}

async fn run_auth_list(config: config::Config) -> anyhow::Result<()> {
    for s in config.servers() {
        println!("{}: {}", s.name, s.uri)
    }

    Ok(())
}

async fn run_auth_info(config: config::Config, instance: Option<String>) -> anyhow::Result<()> {
    let server = match instance {
        Some(i) => config.find_server(&i),
        None => config.default_server(),
    };

    if let Some(s) = server {
        println!("{}: {}", s.name, s.uri);
    }

    Ok(())
}

async fn run_auth_set_default(mut config: config::Config, instance: &String) -> anyhow::Result<()> {
    if config.find_server(instance).is_none() {
        bail!("Authentication for {} does not exist", instance);
    }

    config.set_default(instance);

    config.write().await?;

    Ok(())
}

async fn run_auth_remove(mut config: config::Config, instance: &String) -> anyhow::Result<()> {
    if config.find_server(instance).is_none() {
        bail!("Authentication for {} does not exist", instance);
    }

    config.remove_server(instance);

    config.write().await?;

    Ok(())
}

async fn run_auth_modify(
    mut config: config::Config,
    instance: &str,
    modify_args: AuthModifyArg,
) -> anyhow::Result<()> {
    let current_server = config
        .find_server_mut(instance)
        .context("Authentication for {} does not exist")?;

    if let Some(uri) = modify_args.uri {
        current_server.uri = uri;
    };

    if let Some(token) = modify_args.token {
        current_server.auth = config::Auth::Token(token);
    };

    if let Some(ref token_path) = modify_args.token_file {
        let file = tokio::fs::File::open(token_path).await?;
        let mut reader = BufReader::new(file);
        let mut token = String::new();
        let read = reader.read_line(&mut token).await?;
        // Arbitrary minimal size to make sure might actually be a token without going all the way
        // and parsing it. Mostly to avoid accidentally using an empty file
        if read < 16 {
            bail!("Token file doesn't look like a JWT token (too small)");
        }
        let token = token.trim_end();
        current_server.auth = config::Auth::Token(token.to_string())
    };

    config.write().await?;

    Ok(())
}

#[derive(clap::Parser)]
struct Opts {
    #[clap(short, long)]
    config: Option<PathBuf>,
    /// instance name
    #[clap(short, long)]
    instance: Option<String>,
    #[command(subcommand)]
    command: Command,
}

async fn print_item(
    boardswarm: &mut Boardswarm,
    item_type: ItemType,
    item: &boardswarm_protocol::Item,
    verbose: bool,
) -> anyhow::Result<()> {
    print!("{} {}", item.id, item.name);
    if let Some(ref instance) = item.instance {
        println!(" on {instance}");
    } else {
        println!();
    }
    if verbose {
        let properties = boardswarm.properties(item_type, item.id).await?;
        for key in properties.keys().sorted_unstable() {
            println!(r#""{}" => "{}""#, key, properties[key]);
        }
    }
    Ok(())
}

async fn print_device(
    boardswarm: &mut Boardswarm,
    device: &boardswarm_protocol::Device,
) -> anyhow::Result<()> {
    println!(
        "Current mode: {}",
        device.current_mode.as_deref().unwrap_or("Unknown")
    );
    println!("Modes:");
    for m in &device.modes {
        print!("- {}", m.name);
        if let Some(d) = m.depends.as_ref() {
            print!(" (depends on {d})");
        }
        if m.available {
            println!()
        } else {
            println!(" (Not available)")
        }
    }
    println!("Consoles:");
    for c in &device.consoles {
        if let Some(id) = c.id {
            println!("- {} - id: {}", c.name, id);
        } else {
            println!("- {} - not available", c.name);
        }
    }
    println!("Volumes:");
    for v in &device.volumes {
        if let Some(id) = v.id {
            let info = boardswarm.volume_info(id).await?;
            println!(
                "- {} - id: {}, targets{}:",
                v.name,
                id,
                if info.exhaustive {
                    ""
                } else {
                    " (non-exhaustive)"
                }
            );
            for boardswarm_protocol::VolumeTarget {
                name,
                readable,
                writable,
                seekable,
                size,
                blocksize,
            } in &info.target
            {
                let caps = [
                    (*readable, "readable"),
                    (*writable, "writable"),
                    (*seekable, "seekable"),
                ]
                .iter()
                .filter_map(|(t, v)| if *t { Some(v) } else { None })
                .join(", ");

                print!("  + {name} ({caps})");
                if let Some(size) = size {
                    print!(" size: {size}");
                }
                if let Some(blocksize) = blocksize {
                    print!(" blocksize: {blocksize}");
                }
                println!();
            }
        } else {
            println!("- {} - not available", v.name);
        }
    }
    Ok(())
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let opt = Opts::parse();
    if !matches!(opt.command, Command::Ui { .. }) {
        tracing_subscriber::fmt::init();
    }

    let config_path = opt.config.clone().unwrap_or_else(|| {
        let mut c = dirs::config_dir().expect("Config directory not found");
        c.push("boardswarm");
        c.push("config.yaml");
        c
    });

    let config = match boardswarm_client::config::Config::from_file(&config_path).await {
        Ok(config) => Some(config),
        Err(config::Error::IO(e)) if e.kind() == std::io::ErrorKind::NotFound => None,
        Err(e) => return Err(e).context("Failed to load config"),
    };

    let server = if let Some(ref c) = config {
        if let Some(ref name) = opt.instance {
            c.find_server(name)
        } else {
            c.default_server()
        }
    } else {
        None
    };

    // Pre-connection handling
    let mut boardswarm = match opt.command {
        Command::Auth { command } => {
            let config = config
                .clone()
                .unwrap_or_else(|| config::Config::new(config_path));

            match command {
                AuthCommand::Init(_)
                | AuthCommand::SetDefault
                | AuthCommand::Remove
                | AuthCommand::Modify(_) => {
                    if opt.instance.is_none() {
                        bail!("-i/--instance required for this command.");
                    }
                }
                AuthCommand::Info | AuthCommand::List => (),
            }

            match command {
                AuthCommand::Init(init_args) => {
                    return run_auth_init(config, &opt.instance.unwrap(), init_args).await;
                }
                AuthCommand::Info => {
                    return run_auth_info(config, opt.instance).await;
                }
                AuthCommand::List => {
                    return run_auth_list(config).await;
                }
                AuthCommand::SetDefault => {
                    return run_auth_set_default(config, &opt.instance.unwrap()).await;
                }
                AuthCommand::Remove => {
                    return run_auth_remove(config, &opt.instance.unwrap()).await;
                }
                AuthCommand::Modify(modify_args) => {
                    return run_auth_modify(config, &opt.instance.unwrap(), modify_args).await;
                }
            }
        }
        Command::LoginInfo => {
            if let Some(server) = server {
                server.to_boardswarm_builder().connect().await?
            } else if let Some(ref instance) = opt.instance {
                let Ok(uri) = instance.parse() else {
                    return Err(anyhow!("{instance} not a uri and not in configuration"));
                };
                BoardswarmBuilder::new(uri).connect().await?
            } else {
                return Err(anyhow!(
                    "default Server should be configured or instance passed"
                ));
            }
        }
        _ => {
            if let Some(server) = server {
                let mut builder: BoardswarmBuilder = server.to_boardswarm_builder();
                builder.login_provider(StdoutAuth());
                builder.connect().await?
            } else if config.is_none() {
                return Err(anyhow!(
                    "Configuration file not found; Run configure first?"
                ));
            } else if let Some(ref instance) = opt.instance {
                return Err(anyhow!("{instance} not found in configuration"));
            } else {
                return Err(anyhow!("No default instance configured"));
            }
        }
    };

    match opt.command {
        Command::Auth { .. } => {
            unreachable!()
        }
        Command::LoginInfo => {
            println!("Info: {:#?}", boardswarm.login_info().await?);
            Ok(())
        }
        Command::List { type_, verbose } => {
            let items = boardswarm.list(type_.into()).await?;
            println!("{type_:#}s: ");
            for i in items {
                print_item(&mut boardswarm, type_.into(), &i, verbose).await?;
            }
            Ok(())
        }
        Command::Monitor { type_, verbose } => {
            let events = boardswarm.monitor(type_.into()).await?;
            println!("{type_:#}s: ");
            pin_mut!(events);
            while let Some(event) = events.next().await {
                let event = event?;
                match event {
                    ItemEvent::Added(items) => {
                        for i in items {
                            print_item(&mut boardswarm, type_.into(), &i, verbose).await?;
                        }
                    }
                    ItemEvent::Removed(removed) => println!("Removed: {}", removed),
                }
            }
            Ok(())
        }
        Command::Actuator { actuator, command } => {
            let actuator = item_lookup(actuator, ItemType::Actuator, boardswarm.clone()).await?;
            match command {
                ActuatorCommand::ChangeMode(c) => {
                    let p = serde_json::from_str(&c.mode)
                        .context("Failed to parse actuator mode as JSON")?;

                    boardswarm.actuator_change_mode(actuator, p).await?;
                }
                ActuatorCommand::Properties => {
                    let properties = boardswarm.properties(ItemType::Actuator, actuator).await?;
                    for key in properties.keys().sorted_unstable() {
                        println!(r#""{}" => "{}""#, key, properties[key]);
                    }
                }
            }

            Ok(())
        }
        Command::Console { console, command } => {
            let console = item_lookup(console, ItemType::Console, boardswarm.clone()).await?;
            match command {
                ConsoleCommand::Configure(c) => {
                    let p = serde_json::from_str(&c.configuration)
                        .context("Failed to parse console configuration as JSON")?;
                    boardswarm.console_configure(console, p).await?;
                }
                ConsoleCommand::Tail => {
                    let output = boardswarm.console_stream_output(console).await?;
                    copy_output_to_stdout(output).await?;
                }
                ConsoleCommand::Connect => {
                    let out =
                        copy_output_to_stdout(boardswarm.console_stream_output(console).await?);
                    let in_ = boardswarm.console_stream_input(console, input_stream());
                    futures::select! {
                        in_ = in_.fuse() => in_?,
                        out = out.fuse() => out?,
                    }
                }
                ConsoleCommand::Properties => {
                    let properties = boardswarm.properties(ItemType::Console, console).await?;
                    for key in properties.keys().sorted_unstable() {
                        println!(r#""{}" => "{}""#, key, properties[key]);
                    }
                }
            }

            Ok(())
        }
        Command::Volume { volume, command } => {
            let volume = item_lookup(volume, ItemType::Volume, boardswarm.clone()).await?;
            match command {
                VolumeCommand::Info => {
                    let info = boardswarm.volume_info(volume).await?;
                    if info.exhaustive {
                        println!("Volume targets:");
                    } else {
                        println!("Volume targets (non-exhaustive):");
                    }

                    for target in &info.target {
                        print!("* {}: ", target.name);
                        if let Some(size) = target.size {
                            print!("size: {size}, ");
                        } else {
                            print!("size: unknown, ");
                        }
                        if let Some(blocksize) = target.blocksize {
                            print!("blocksize: {blocksize}, ");
                        } else {
                            print!("blocksize: unknown, ");
                        }
                        println!(
                            "readable: {}, writable: {}, seekable: {}",
                            target.readable, target.writable, target.seekable
                        );
                    }
                }
                VolumeCommand::Write(write) => {
                    let mut f = tokio::fs::File::open(write.file).await?;
                    let m = f.metadata().await?;
                    let rw = boardswarm
                        .volume_io_readwrite(volume, write.target, Some(m.len()))
                        .await?;
                    let progress = ProgressBar::new(m.len());
                    let mut batchwriter = BatchWriter::new(rw).discard_flush();
                    let mut rw = progress
                        .wrap_async_write(&mut batchwriter)
                        .compat_write()
                        .into_inner();
                    if let Some(offset) = write.offset {
                        rw.seek(SeekFrom::Start(offset)).await?;
                    }
                    tokio::io::copy(&mut f, &mut rw).await?;
                    rw.shutdown().await?;
                    drop(rw);
                    progress.finish();
                }
                VolumeCommand::WriteAimg(write) => {
                    let rw = boardswarm
                        .volume_io_readwrite(volume, write.target, None)
                        .await?;
                    let rw = BatchWriter::new(rw).discard_flush();
                    write_aimg(rw, &write.file).await?;
                }
                VolumeCommand::WriteBmap(write) => {
                    let rw = boardswarm
                        .volume_io_readwrite(volume, write.target, None)
                        .await?;
                    write_bmap(rw, &write.file).await?;
                }
                VolumeCommand::Commit => {
                    boardswarm.volume_commit(volume).await?;
                }
                VolumeCommand::Erase { target } => {
                    boardswarm.volume_erase(volume, target).await?;
                }
                VolumeCommand::Properties => {
                    let properties = boardswarm.properties(ItemType::Volume, volume).await?;
                    for key in properties.keys().sorted_unstable() {
                        println!(r#""{}" => "{}""#, key, properties[key]);
                    }
                }
            }
            Ok(())
        }
        Command::Device { device, command } => {
            let device = device.device(boardswarm.clone()).await?;
            let device = device.ok_or_else(|| anyhow::anyhow!("Device not found"))?;
            match command {
                DeviceCommand::Read(DeviceReadArg {
                    offset,
                    length,
                    target,
                    file,
                }) => {
                    let mut f = tokio::fs::File::create(file).await?;
                    let (_volume, mut r) = target.open(&device).await?;
                    if let Some(offset) = offset {
                        r.seek(SeekFrom::Start(offset)).await?;
                    }

                    // For most volumes reading 8K at a time will be rather slow, so use a 1
                    // megabyte reader
                    let mut r = BufReader::with_capacity(1024 * 1024, r);
                    if let Some(length) = length {
                        let mut r = r.take(length);
                        tokio::io::copy_buf(&mut r, &mut f).await?;
                    } else {
                        tokio::io::copy_buf(&mut r, &mut f).await?;
                    }
                    f.flush().await?;
                }
                DeviceCommand::Write(DeviceWriteArg {
                    offset,
                    commit,
                    target,
                    file,
                }) => {
                    let mut f = tokio::fs::File::open(file).await?;
                    let m = f.metadata().await?;

                    let (mut volume, rw) = target.open_with_len(&device, m.len()).await?;
                    let progress = ProgressBar::new(m.len());
                    let mut batchwriter = BatchWriter::new(rw).discard_flush();
                    let mut rw = progress
                        .wrap_async_write(&mut batchwriter)
                        .compat_write()
                        .into_inner();
                    if let Some(offset) = offset {
                        rw.seek(SeekFrom::Start(offset)).await?;
                    }

                    tokio::io::copy(&mut f, &mut rw).await?;
                    rw.shutdown().await.context("Volume shutdown")?;
                    drop(rw);

                    if commit {
                        volume.commit().await?;
                    }
                    progress.finish();
                }
                DeviceCommand::WriteAimg(DeviceAimgWriteArg {
                    commit,
                    target,
                    file,
                }) => {
                    let (mut volume, rw) = target.open(&device).await?;
                    let rw = BatchWriter::new(rw).discard_flush();
                    write_aimg(rw, &file).await?;

                    if commit {
                        volume.commit().await?;
                    }
                }
                DeviceCommand::WriteBmap(DeviceBmapWriteArg {
                    commit,
                    target,
                    file,
                }) => {
                    let (mut volume, rw) = target.open(&device).await?;
                    write_bmap(rw, &file).await?;

                    if commit {
                        volume.commit().await?;
                    }
                }
                DeviceCommand::Erase(DeviceEraseArg { volume, target }) => {
                    let mut volume = volume.open(&device).await?;
                    volume.erase(target).await?;
                }
                DeviceCommand::Commit(volume) => {
                    let mut volume = volume.open(&device).await?;
                    volume.commit().await?;
                }
                DeviceCommand::Info { follow } => {
                    let mut d = boardswarm.device_info(device.id()).await?;
                    while let Some(device) = d.try_next().await? {
                        print_device(&mut boardswarm, &device).await?;
                        if !follow {
                            break;
                        }
                        println!("---------");
                    }
                }
                DeviceCommand::Mode(d) => {
                    device.change_mode(d.mode).await?;
                }
                DeviceCommand::Reset => {
                    println!("Turning off");
                    device.change_mode("off").await?;
                    println!("Turning on");
                    device.change_mode("on").await?;
                }
                DeviceCommand::Connect(d) => {
                    let mut console = if let Some(c) = &d.console {
                        device
                            .console_by_name(c)
                            .ok_or_else(|| anyhow::anyhow!("Console not found"))?
                    } else {
                        device
                            .console()
                            .ok_or_else(|| anyhow::anyhow!("Console not found"))?
                    };
                    let out = copy_output_to_stdout(console.stream_output().await?);
                    let in_ = console.stream_input(input_stream());
                    futures::select! {
                        in_ = in_.fuse() => in_?,
                        out = out.fuse() => out?,
                    }
                }
                DeviceCommand::Tail(d) => {
                    let mut console = if let Some(c) = &d.console {
                        device
                            .console_by_name(c)
                            .ok_or_else(|| anyhow::anyhow!("Console not found"))?
                    } else {
                        device
                            .console()
                            .ok_or_else(|| anyhow::anyhow!("Console not found"))?
                    };
                    let output = console.stream_output().await?;
                    copy_output_to_stdout(output).await?;
                }
                DeviceCommand::Properties => {
                    let properties = boardswarm.properties(ItemType::Device, device.id()).await?;
                    for key in properties.keys().sorted_unstable() {
                        println!(r#""{}" => "{}""#, key, properties[key]);
                    }
                }
            }
            Ok(())
        }
        Command::Rock { device, command } => {
            let device = device
                .device(boardswarm)
                .await?
                .ok_or_else(|| anyhow::anyhow!("Device not found"))?;
            match command {
                RockCommand::DownloadBoot { wait, volume, path } => {
                    let mut volume = if let Some(volume) = volume {
                        device
                            .volume_by_name(&volume)
                            .ok_or_else(|| anyhow::anyhow!("Volume not found"))?
                    } else {
                        let mut volumes = device.volumes();
                        match volumes.len().cmp(&1) {
                            Ordering::Equal => volumes.pop().unwrap(),
                            Ordering::Greater => bail!("More then one volume, please specify one"),
                            Ordering::Less => bail!("No volumes for this device"),
                        }
                    };
                    if !volume.available() {
                        if wait {
                            println!("Waiting for volume..");
                            volume.wait().await;
                        } else {
                            bail!("volume not available");
                        }
                    }
                    rock_download_boot(&mut volume, &path).await?;
                    volume.wait_unavailable().await;
                }
            }
            Ok(())
        }
        Command::Ui {
            device,
            console,
            terminal_size,
            scrollback_lines,
        } => {
            let device = device
                .device(boardswarm)
                .await?
                .ok_or_else(|| anyhow::anyhow!("Device not found"))?;

            ui::run_ui(device, console.console, terminal_size, scrollback_lines).await
        }
    }
}
