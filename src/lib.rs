
use std::str::FromStr;

use fipc::server::ServerName;
use fmp_interface::{SERVER_NAME, client_name::ClientNameError, command::{ClientCommand, Command}, message::Message};
use thiserror::Error;
use tokio::{io::{self, AsyncBufReadExt, AsyncWriteExt, BufReader, BufWriter, Lines, ReadHalf}, select, sync::{mpsc, watch}, task::{JoinError, JoinHandle}};

#[derive(Error, Debug)]
pub enum Error {
    #[error("Disconnected from server unexpectedly")]
    Disconnected,
    #[error("IO Error: {0}")]
    IOError(#[from] io::Error),
    #[error("Task panic: {0}")]
    Panic(#[from] JoinError),
    #[error("Task has terminated unexpectedly")]
    TaskTerminated,
}

#[derive(Debug, Error)]
pub enum IdentificationError {
    #[error(transparent)]
    Error(#[from] Error),
    #[error("Client name error: {0}")]
    NameError(#[from] ClientNameError),
}

pub enum Event {
    Message(Message),
    InvalidMessage(<Message as FromStr>::Err),
}

pub struct Client {
    name: String,
    stream: Lines<BufReader<ReadHalf<fipc::client::Client>>>,
    task: JoinHandle<io::Result<!>>,
    commands: mpsc::UnboundedSender<Command>,
    watch: watch::Receiver<bool>,
}

impl Client {
    pub async fn connect() -> io::Result<Self> {
        let client = fipc::client::Client::connect(&ServerName::new(SERVER_NAME).unwrap()).await?;
        let (reader, writer) = io::split(client);
        let (sender, mut receiver) = mpsc::unbounded_channel();
        let (watch_sender, watch_receiver) = watch::channel(false);
        
        Ok(Self {
            name: String::new(),
            stream: BufReader::new(reader).lines(),
            commands: sender,
            task: tokio::spawn(async move {
                let mut writer = BufWriter::new(writer);
                let mut active = false;
                
                loop {
                    let Some(command) = receiver.recv().await else { break };
                    
                    if !active {
                        active = true;
                        let _ = watch_sender.send(true);
                    }
                    
                    writer.write_all(command.to_line().as_bytes()).await?;
                    
                    if receiver.is_empty() {
                        writer.flush().await?;
                        active = false;
                        let _ = watch_sender.send(false);
                    }
                }
                
                std::future::pending().await
            }),
            watch: watch_receiver,
        })
    }
    
    pub fn name(&self) -> Option<&str> { if self.name.is_empty() { None } else { Some(&self.name) } }
    
    pub async fn identify(&mut self, name: impl Into<String>) -> Result<Option<ClientNameError>, Error> {
        let name = name.into();
        
        self.command(ClientCommand::Identify { name: name.clone() })?;
        
        loop {
            match self.listen().await? {
                Event::Message(Message::Identified) => {
                    self.name = name;
                    return Ok(None);
                }
                Event::Message(Message::IdentificationError(e)) => return Ok(Some(e)),
                _ => (),
            }
        }
    }
    
    pub async fn listen(&mut self) -> Result<Event, Error> {
        let Self { stream, task, .. } = self;
        
        select! {
            message = stream.next_line() => {
                let Some(message) = message? else { return Err(Error::Disconnected) };
                
                Ok(match Message::from_str(&message) {
                    Ok(message) => Event::Message(message),
                    Err(e) => Event::InvalidMessage(e),
                })
            }
            error = task => match error {
                Ok(Err(e)) => Err(e)?,
                Err(e) => Err(e)?,
            },
        }
    }
    
    pub fn command(&mut self, command: impl Into<Command>) -> Result<(), Error> {
        self.commands.send(command.into()).map_err(|_| Error::TaskTerminated)
    }
    
    pub async fn wait_commands(&mut self) -> Result<(), Error> {
        self.watch.wait_for(|active| !active).await.map(|_| ()).map_err(|_| Error::TaskTerminated)
    }
}
