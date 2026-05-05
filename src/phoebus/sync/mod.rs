//! Phoebus Synchronization Module
//!
//! Handles how updates from Phoebus are communicated to the Controls alarms server.

use std::sync::Arc;

use tokio::sync::{Mutex, RwLock};
use tonic::transport::Channel;
use tracing::{debug, error, info, warn};

use crate::models::OutboundSyncResult;
use crate::models::alarm::alarm_commands_client::AlarmCommandsClient;
use crate::models::alarm::{AcknowledgeRequest, ActivateRequest, BypassRequest, SnoozeRequest};
use crate::models::generated::Timestamp;

#[cfg(test)]
mod tests;

/// Holds a shared gRPC client and a monotonically increasing generation counter used to detect stale reconnect attempts.
#[derive(Clone)]
struct SharedConnectionState {
    /// The shared gRPC client for issuing alarm commands to the Controls service.
    client: AlarmCommandsClient<Channel>,
    /// A monotonically increasing counter incremented each time the client is replaced after a failure.
    generation: u64,
}

/// Manages the lifecycle of the shared gRPC client, including lazy initialization and reconnection after failures.
struct ConnectionManager {
    /// The current shared connection state, wrapped in a read-write lock for concurrent access.
    connection: Arc<RwLock<Option<SharedConnectionState>>>,
    /// A mutex used to serialize reconnection attempts and prevent duplicate client creation.
    connection_gate: Arc<Mutex<()>>,
    /// The host address of the Controls gRPC alarms service.
    grpc_alarms_svc_host: String,
}

/// A short-lived snapshot of the shared client and its generation, used for a single outbound command attempt.
#[derive(Clone)]
struct RequestClient {
    /// The gRPC client to use for the outbound command.
    client: AlarmCommandsClient<Channel>,
    /// The generation of the shared client at the time this snapshot was taken, used to detect stale reconnects.
    generation: Option<u64>,
}

/// Handles the reference to the global [`AlarmCommandsClient`] instance and provides methods to interact
/// with the client to issue commands.
pub struct ControlsClient {
    connection_manager: Arc<ConnectionManager>,
}

impl ControlsClient {
    /// Creates a new [`ControlsClient`] that will connect to the Controls gRPC alarms service at the provided host.
    pub fn new(grpc_alarms_svc_host: &str) -> Self {
        Self {
            connection_manager: Arc::new(ConnectionManager::new(grpc_alarms_svc_host)),
        }
    }

    /// Sends the command to the Controls alarms server to acknowledge the alarming device.
    pub async fn acknowledge_alarm(&self, device: &str, user: &str) -> OutboundSyncResult {
        let request = AcknowledgeRequest {
            devices: vec![format!("{device}#Epics")],
            user: user.to_string(),
        };
        self.execute_command("acknowledge", request).await
    }

    /// Sends the command to the Controls alarms server to activate (re-enable) the alarming device.
    pub async fn activate_alarm(&self, device: &str, user: &str) -> OutboundSyncResult {
        let request = ActivateRequest {
            devices: vec![format!("{device}#Epics")],
            user: user.to_string(),
        };
        self.execute_command("activate", request).await
    }

    /// Sends the command to the Controls alarms server to bypass the alarming device.
    pub async fn bypass_alarm(&self, device: &str, user: &str) -> OutboundSyncResult {
        let request = BypassRequest {
            devices: vec![format!("{device}#Epics")],
            user: user.to_string(),
        };
        self.execute_command("bypass", request).await
    }

    /// Sends the command to the Controls alarms server to snooze the alarming device.
    pub async fn snooze_alarm(
        &self,
        device: &str,
        user: &str,
        wake: Timestamp,
    ) -> OutboundSyncResult {
        let request = SnoozeRequest {
            devices: vec![format!("{device}#Epics")],
            user: user.to_string(),
            wake: Some(wake),
        };
        self.execute_command("snooze", request).await
    }

    /// Executes a gRPC command against the Controls alarms service, retrying once with a fresh connection on failure.
    ///
    /// If both the initial attempt and the reconnect retry fail, returns [`OutboundSyncResult::Failed`].
    async fn execute_command<Request>(
        &self,
        command_label: &'static str,
        request: Request,
    ) -> OutboundSyncResult
    where
        Request: Clone,
        AlarmCommandsClient<Channel>: CommandRequest<Request>,
    {
        let RequestClient {
            mut client,
            generation,
        } = match self.connection_manager.request_client().await {
            Some(client) => client,
            None => return OutboundSyncResult::Failed,
        };

        debug!(
            generation = generation,
            "Using request-ready Controls gRPC client for outbound command"
        );

        if Self::run_operation(&mut client, request.clone()).await {
            return OutboundSyncResult::Succeeded;
        }

        let mut replacement_client =
            match self.connection_manager.reconnect_client(generation).await {
                Some(client) => client,
                None => return OutboundSyncResult::Failed,
            };

        if Self::run_operation(&mut replacement_client, request).await {
            return OutboundSyncResult::Succeeded;
        }

        let final_generation = self.connection_manager.current_generation().await;
        error!(
            generation = final_generation,
            command = command_label,
            "Outbound command exhausted reconnect retry"
        );
        OutboundSyncResult::Failed
    }

    /// Sends a single gRPC request using the provided client and returns `true` if it succeeded.
    async fn run_operation<Request>(
        conn: &mut AlarmCommandsClient<Channel>,
        request: Request,
    ) -> bool
    where
        AlarmCommandsClient<Channel>: CommandRequest<Request>,
    {
        match conn.send_request(request).await {
            Ok(_) => true,
            Err(error) => {
                error!("Failed to send command to Controls alarms service: {error}");
                false
            }
        }
    }
}

impl Clone for ControlsClient {
    fn clone(&self) -> Self {
        Self {
            connection_manager: Arc::clone(&self.connection_manager),
        }
    }
}

impl ConnectionManager {
    /// Creates a new [`ConnectionManager`] that will lazily connect to the provided host on first use.
    fn new(grpc_alarms_svc_host: &str) -> Self {
        Self {
            connection: Arc::new(RwLock::new(None)),
            connection_gate: Arc::new(Mutex::new(())),
            grpc_alarms_svc_host: grpc_alarms_svc_host.to_owned(),
        }
    }

    /// Returns a [`RequestClient`] snapshot ready for a single outbound command attempt.
    ///
    /// Lazily initializes the shared connection if one does not yet exist. Returns `None` if the connection cannot be established.
    async fn request_client(&self) -> Option<RequestClient> {
        let client = self.get_or_connect_client().await?;
        let generation = self.current_generation().await;
        Some(RequestClient { client, generation })
    }

    /// Returns the existing shared client if one is available, or establishes a new connection.
    ///
    /// Uses a double-checked locking pattern to avoid redundant connection attempts under concurrent load.
    async fn get_or_connect_client(&self) -> Option<AlarmCommandsClient<Channel>> {
        if let Some(conn) = self.connection.read().await.as_ref().cloned() {
            debug!(
                generation = conn.generation,
                "Reusing existing shared Controls gRPC client for outbound command"
            );
            return Some(conn.client);
        }

        let _connection_gate = self.connection_gate.lock().await;

        if let Some(conn) = self.connection.read().await.as_ref().cloned() {
            debug!(
                generation = conn.generation,
                "Reusing existing shared Controls gRPC client for outbound command"
            );
            return Some(conn.client);
        }

        info!("Initializing shared Controls gRPC client");
        let client = match AlarmCommandsClient::connect(self.grpc_alarms_svc_host.clone()).await {
            Ok(client) => client,
            Err(e) => {
                error!("Failed to connect to Controls alarms service: {e}");
                return None;
            }
        };

        self.publish_client(client.clone(), 1).await;
        info!(
            generation = 1u64,
            "Outbound command triggered initial shared Controls gRPC client creation"
        );
        Some(client)
    }

    /// Attempts to replace the shared client after a command failure.
    ///
    /// If another task has already published a newer client generation, the existing newer client is returned
    /// instead of creating a redundant connection. Returns `None` if reconnection fails.
    async fn reconnect_client(
        &self,
        failed_generation: Option<u64>,
    ) -> Option<AlarmCommandsClient<Channel>> {
        warn!(
            generation = failed_generation,
            "Outbound command failed before reconnect; retry recovery will be attempted"
        );

        let _connection_gate = self.connection_gate.lock().await;

        if let Some(state) = self.connection.read().await.as_ref().cloned()
            && Some(state.generation) != failed_generation
        {
            info!(
                generation = state.generation,
                "Skipped duplicate reconnect because a newer shared Controls gRPC client was already published"
            );
            return Some(state.client);
        }

        let next_generation = failed_generation.unwrap_or(0).saturating_add(1);
        warn!(
            failed_generation = failed_generation,
            next_generation = next_generation,
            "Reconnecting shared Controls gRPC client after command failure"
        );
        info!(
            generation = next_generation,
            "Starting reconnect attempt for shared Controls gRPC client"
        );

        let client = match AlarmCommandsClient::connect(self.grpc_alarms_svc_host.clone()).await {
            Ok(client) => client,
            Err(connect_error) => {
                error!(
                    "Failed to reconnect to Controls alarms service after command failure: {connect_error}"
                );
                return None;
            }
        };

        self.publish_client(client.clone(), next_generation).await;
        Some(client)
    }

    /// Returns the generation counter of the current shared client, or `None` if no client has been established yet.
    async fn current_generation(&self) -> Option<u64> {
        self.connection
            .read()
            .await
            .as_ref()
            .map(|state| state.generation)
    }

    /// Stores the provided client as the new shared connection state with the given generation counter.
    async fn publish_client(&self, client: AlarmCommandsClient<Channel>, generation: u64) {
        let mut lock = self.connection.write().await;
        *lock = Some(SharedConnectionState { client, generation });

        debug!(
            generation = generation,
            "Published shared Controls gRPC client generation"
        );
    }
}

/// A generic abstraction over the gRPC request methods exposed by [`AlarmCommandsClient`].
///
/// Allows [`ControlsClient::execute_command`] to be generic over the specific request type
/// without duplicating the retry and reconnect logic for each command variant.
#[async_trait::async_trait]
trait CommandRequest<Request> {
    /// Sends the provided request to the Controls alarms service.
    async fn send_request(&mut self, request: Request) -> Result<(), tonic::Status>;
}

#[async_trait::async_trait]
impl CommandRequest<AcknowledgeRequest> for AlarmCommandsClient<Channel> {
    async fn send_request(&mut self, request: AcknowledgeRequest) -> Result<(), tonic::Status> {
        self.acknowledge(request).await?;
        Ok(())
    }
}

#[async_trait::async_trait]
impl CommandRequest<ActivateRequest> for AlarmCommandsClient<Channel> {
    async fn send_request(&mut self, request: ActivateRequest) -> Result<(), tonic::Status> {
        self.activate(request).await?;
        Ok(())
    }
}

#[async_trait::async_trait]
impl CommandRequest<BypassRequest> for AlarmCommandsClient<Channel> {
    async fn send_request(&mut self, request: BypassRequest) -> Result<(), tonic::Status> {
        self.bypass(request).await?;
        Ok(())
    }
}

#[async_trait::async_trait]
impl CommandRequest<SnoozeRequest> for AlarmCommandsClient<Channel> {
    async fn send_request(&mut self, request: SnoozeRequest) -> Result<(), tonic::Status> {
        self.snooze(request).await?;
        Ok(())
    }
}
