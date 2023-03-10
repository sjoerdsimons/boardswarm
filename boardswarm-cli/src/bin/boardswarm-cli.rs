use std::{
    convert::Infallible,
    os::unix::prelude::AsRawFd,
    path::{Path, PathBuf},
};

use anyhow::{anyhow, bail};
use boardswarm_cli::client::{Boardswarm, UploadProgressState};
use boardswarm_protocol::ItemType;
use bytes::{Bytes, BytesMut};
use clap::{arg, Args, Parser, Subcommand};
use futures::{pin_mut, stream, FutureExt, Stream, StreamExt, TryStreamExt};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

use boardswarm_cli::client::ItemEvent;

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
    let stdin_fd = stdin.as_raw_fd();

    let mut stdin_termios = nix::sys::termios::tcgetattr(stdin_fd).unwrap();

    nix::sys::termios::cfmakeraw(&mut stdin_termios);
    nix::sys::termios::tcsetattr(stdin_fd, nix::sys::termios::SetArg::TCSANOW, &stdin_termios)
        .unwrap();

    futures::stream::unfold(stdin, |mut stdin| async move {
        let mut data = BytesMut::zeroed(64);
        let r = stdin.read(&mut data).await.ok()?;
        data.truncate(r);
        Some((data.into(), stdin))
    })
}

async fn file_stream(path: &Path) -> anyhow::Result<(u64, impl Stream<Item = Bytes>)> {
    let f = tokio::fs::File::open(path).await?;
    let m = f.metadata().await?;
    let data = stream::unfold(f, |mut f| async move {
        let mut data = BytesMut::zeroed(4096);
        let r = f.read(&mut data).await.unwrap();
        if r == 0 {
            None
        } else {
            data.truncate(r);
            Some((data.freeze(), f))
        }
    });
    Ok((m.len(), data))
}

async fn watch_upload_progress(
    progress: boardswarm_cli::client::UploadProgress,
    length: u64,
) -> anyhow::Result<()> {
    let bar = indicatif::ProgressBar::new(length);
    bar.set_style(
        indicatif::ProgressStyle::default_bar()
            .template(
                "[{bar:40.cyan/blue}]   \
                          {bytes}/{total_bytes} ({bytes_per_sec}) ({eta})",
            )?
            .progress_chars("#>-"),
    );

    let mut stream = progress.stream();

    while let Some(progress) = stream.next().await {
        match progress {
            UploadProgressState::Writing(written) => bar.set_position(written),
            UploadProgressState::Succeeded => bar.finish(),
            UploadProgressState::Failed(e) => {
                bar.abandon();
                return Err(e.into());
            }
        }
    }

    Ok(())
}

#[derive(Debug, Args)]
struct ActuatorMode {
    actuator: u64,
    mode: String,
}

#[derive(Debug, Subcommand)]
enum ActuatorCommand {
    /// Change actuator mode
    ChangeMode(ActuatorMode),
}

#[derive(Debug, Args)]
struct ConsoleArgs {
    console: u64,
}

#[derive(Debug, Args)]
struct ConsoleConfigure {
    console: u64,
    configuration: String,
}

#[derive(Debug, Subcommand)]
enum ConsoleCommand {
    /// Configure a console
    Configure(ConsoleConfigure),
    /// Tail the output of a device console
    Tail(ConsoleArgs),
    /// Connect input and output to a device console
    Connect(ConsoleArgs),
}

#[derive(Debug, Args)]
struct UploadArgs {
    uploader: u64,
    target: String,
    file: PathBuf,
}

#[derive(Debug, Subcommand)]
enum UploadCommand {
    Info {
        uploader: u64,
    },
    /// Upload file to uploader target
    Upload(UploadArgs),
    /// Commit upload
    Commit {
        uploader: u64,
    },
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
    ) -> Result<Option<boardswarm_cli::device::Device>, anyhow::Error> {
        let builder = boardswarm_cli::device::DeviceBuilder::from_client(client);
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

#[derive(Debug, Args)]
struct DeviceConsoleArgs {
    #[clap(short, long)]
    console: Option<String>,
    #[arg(value_parser = parse_device)]
    device: DeviceArg,
}

#[derive(Debug, Args)]
struct DeviceModeArgs {
    #[arg(value_parser = parse_device)]
    device: DeviceArg,
    mode: String,
}

#[derive(Debug, Subcommand)]
enum DeviceCommand {
    /// Get info about a device
    Info {
        #[arg(value_parser = parse_device)]
        device: DeviceArg,
    },
    Upload {
        #[arg(short, long)]
        wait: bool,
        #[arg(short, long)]
        commit: bool,
        #[arg(value_parser = parse_device)]
        device: DeviceArg,
        uploader: String,
        target: String,
        file: PathBuf,
    },
    /// Change device mode
    Mode(DeviceModeArgs),
    // Turn the device off and on again
    Reset {
        #[arg(value_parser = parse_device)]
        device: DeviceArg,
    },
    /// Connect to the console
    Connect(DeviceConsoleArgs),
    /// Tail to the console
    Tail(DeviceConsoleArgs),
}

fn parse_item(item: &str) -> Result<ItemType, anyhow::Error> {
    let types = [
        ("actuators", ItemType::Actuator),
        ("consoles", ItemType::Console),
        ("devices", ItemType::Device),
        ("uploaders", ItemType::Uploader),
    ];
    for (n, t) in types {
        if n == item {
            return Ok(t);
        }
    }

    Err(anyhow::anyhow!(
        "Unknown item type; known types: {:?}",
        types.map(|(n, _)| n)
    ))
}

#[derive(Debug, Subcommand)]
enum Command {
    Actuator {
        #[command(subcommand)]
        command: ActuatorCommand,
    },
    Console {
        #[command(subcommand)]
        command: ConsoleCommand,
    },
    Uploader {
        #[command(subcommand)]
        command: UploadCommand,
    },
    Device {
        #[command(subcommand)]
        command: DeviceCommand,
    },
    Ui(DeviceConsoleArgs),
    List {
        #[arg(value_parser = parse_item)]
        type_: ItemType,
    },
    Monitor {
        #[arg(value_parser = parse_item)]
        type_: ItemType,
    },
    Properties {
        #[arg(value_parser = parse_item)]
        type_: ItemType,
        item: u64,
    },
}

#[derive(clap::Parser)]
struct Opts {
    #[clap(short, long, default_value = "http://localhost:6653")]
    uri: tonic::transport::Uri,
    #[command(subcommand)]
    command: Command,
}

fn print_item(i: boardswarm_protocol::Item) {
    print!("{} {}", i.id, i.name);
    if let Some(instance) = i.instance {
        println!(" on {instance}");
    } else {
        println!();
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let opt = Opts::parse();

    println!("Connecting to: {}", opt.uri);
    let mut boardswarm = boardswarm_cli::client::Boardswarm::connect(opt.uri).await?;

    match opt.command {
        Command::List { type_ } => {
            println!("{:?}s: ", type_);
            for i in boardswarm.list(type_).await? {
                print_item(i);
            }
            Ok(())
        }
        Command::Monitor { type_ } => {
            println!("{:?}s: ", type_);
            let events = boardswarm.monitor(type_).await?;
            pin_mut!(events);
            while let Some(event) = events.next().await {
                let event = event?;
                match event {
                    ItemEvent::Added(items) => {
                        for i in items {
                            print_item(i)
                        }
                    }
                    ItemEvent::Removed(removed) => println!("Removed: {}", removed),
                }
            }
            Ok(())
        }
        Command::Properties { type_, item } => {
            let properties = boardswarm.properties(type_, item).await?;
            for (k, v) in properties {
                println!(r#""{}" => "{}""#, k, v);
            }
            Ok(())
        }
        Command::Actuator { command } => {
            match command {
                ActuatorCommand::ChangeMode(c) => {
                    let p = serde_json::from_str(&c.mode)?;
                    boardswarm.actuator_change_mode(c.actuator, p).await?;
                }
            }

            Ok(())
        }
        Command::Console { command } => {
            match command {
                ConsoleCommand::Configure(c) => {
                    let p = serde_json::from_str(&c.configuration)?;
                    boardswarm.console_configure(c.console, p).await?;
                }
                ConsoleCommand::Tail(c) => {
                    let output = boardswarm.console_stream_output(c.console).await?;
                    copy_output_to_stdout(output).await?;
                }
                ConsoleCommand::Connect(c) => {
                    let out =
                        copy_output_to_stdout(boardswarm.console_stream_output(c.console).await?);
                    let in_ = boardswarm.console_stream_input(c.console, input_stream());
                    futures::select! {
                        in_ = in_.fuse() => in_?,
                        out = out.fuse() => out?,
                    }
                }
            }

            Ok(())
        }
        Command::Uploader { command } => {
            match command {
                UploadCommand::Info { uploader } => {
                    let info = boardswarm.uploader_info(uploader).await?;
                    println!("{:#?}", info);
                }
                UploadCommand::Upload(upload) => {
                    let (length, data) = file_stream(&upload.file).await?;
                    let progress = boardswarm
                        .uploader_upload(upload.uploader, upload.target, data, length)
                        .await?;
                    watch_upload_progress(progress, length).await?;
                }
                UploadCommand::Commit { uploader } => {
                    boardswarm.uploader_commit(uploader).await?;
                }
            }
            Ok(())
        }
        Command::Device { command } => {
            match command {
                DeviceCommand::Upload {
                    wait,
                    commit,
                    device,
                    uploader,
                    target,
                    file,
                } => {
                    let device = device.device(boardswarm).await?;
                    let device = device.ok_or_else(|| anyhow::anyhow!("Device not found"))?;
                    let (length, data) = file_stream(&file).await?;
                    let mut uploader = device
                        .uploader_by_name(&uploader)
                        .ok_or_else(|| anyhow!("Uploader not available for device"))?;
                    if !uploader.available() {
                        if wait {
                            println!("Waiting for uploader..");
                            uploader.wait().await;
                        } else {
                            bail!("uploader not available");
                        }
                    }
                    let progress = uploader.upload(target, data, length).await?;
                    watch_upload_progress(progress, length).await?;
                    println!("{} uploaded", file.display());
                    if commit {
                        uploader.commit().await?;
                    }
                }
                DeviceCommand::Info { device } => {
                    let device = device.device(boardswarm.clone()).await?;
                    let device = device.ok_or_else(|| anyhow::anyhow!("Device not found"))?;
                    let mut d = boardswarm.device_info(device.id()).await?;
                    while let Some(device) = d.try_next().await? {
                        println!("{:#?}", device);
                    }
                }
                DeviceCommand::Mode(d) => {
                    let device = d.device.device(boardswarm).await?;
                    let device = device.ok_or_else(|| anyhow::anyhow!("Device not found"))?;
                    device.change_mode(d.mode).await?;
                }
                DeviceCommand::Reset { device } => {
                    let device = device.device(boardswarm).await?;
                    let device = device.ok_or_else(|| anyhow::anyhow!("Device not found"))?;
                    println!("Turning off");
                    device.change_mode("off").await?;
                    println!("Turning on");
                    device.change_mode("on").await?;
                }
                DeviceCommand::Connect(d) => {
                    let device = d.device.device(boardswarm).await?;
                    let device = device.ok_or_else(|| anyhow::anyhow!("Device not found"))?;
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
                    let device = d.device.device(boardswarm).await?;
                    let device = device.ok_or_else(|| anyhow::anyhow!("Device not found"))?;
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
            }
            Ok(())
        }
        Command::Ui(ui) => {
            let device = ui.device.device(boardswarm).await?;
            let device = device.ok_or_else(|| anyhow::anyhow!("Device not found"))?;
            boardswarm_cli::ui::run_ui(device, ui.console).await
        }
    }
}
