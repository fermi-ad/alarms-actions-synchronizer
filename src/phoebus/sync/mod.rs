//! Phoebus Synchronization Module
//!
//! Handles how updates from Phoebus are communicated to the Controls alarms server.
//!
//! The available Controls gRPC surface is intentionally incomplete for Phoebus-driven synchronization.
//! The shared interfaces repository currently exposes commands for acknowledge, bypass, and snooze, but it does
//! not yet expose an API for reporting that a Phoebus alarm has become active/OK again. That gap is handled in
//! [`Monitor::record_active_alarm_locally_until_controls_supports_it()`](src/phoebus/monitor/mod.rs:110) as an explicit upstream dependency rather than local unfinished work.

#[cfg(test)]
mod tests;

use crate::models::OutboundSyncResult;
use crate::models::alarm::alarm_commands_client::AlarmCommandsClient;
use crate::models::alarm::{AcknowledgeAlarmRequest, BypassAlarmRequest, SnoozeAlarmRequest};
use crate::models::generated::Timestamp;
use std::sync::Arc;
use tokio::sync::{Mutex, RwLock};
use tonic::transport::Channel;
use tracing::{debug, error, info, warn};

#[derive(Clone)]
struct SharedConnectionState {
    client: AlarmCommandsClient<Channel>,
    generation: u64,
}

struct ConnectionManager {
    connection: Arc<RwLock<Option<SharedConnectionState>>>,
    connection_gate: Arc<Mutex<()>>,
    grpc_alarms_svc_host: String,
}

impl ConnectionManager {
    fn new(grpc_alarms_svc_host: &str) -> Self {
        Self {
            connection: Arc::new(RwLock::new(None)),
            connection_gate: Arc::new(Mutex::new(())),
            grpc_alarms_svc_host: grpc_alarms_svc_host.to_owned(),
        }
    }

    async fn request_client(&self) -> Option<RequestClient> {
        let client = self.get_or_connect_client().await?;
        let generation = self.current_generation().await;
        Some(RequestClient { client, generation })
    }

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

    async fn current_generation(&self) -> Option<u64> {
        self.connection
            .read()
            .await
            .as_ref()
            .map(|state| state.generation)
    }

    async fn publish_client(&self, client: AlarmCommandsClient<Channel>, generation: u64) {
        let mut lock = self.connection.write().await;
        *lock = Some(SharedConnectionState { client, generation });

        debug!(
            generation = generation,
            "Published shared Controls gRPC client generation"
        );
    }
}

#[derive(Clone)]
struct RequestClient {
    client: AlarmCommandsClient<Channel>,
    generation: Option<u64>,
}

/// Handles the reference to the global [`AlarmCommandsClient`] instance and provides methods to interact
/// with the client to issue commands.
pub struct ControlsClient {
    connection_manager: Arc<ConnectionManager>,
}

impl ControlsClient {
    pub fn new(grpc_alarms_svc_host: &str) -> Self {
        Self {
            connection_manager: Arc::new(ConnectionManager::new(grpc_alarms_svc_host)),
        }
    }

    /// Sends the command to the Controls alarms server to acknowledge the alarming device.
    pub async fn acknowledge_alarm(&self, device: &str, user: &str) -> OutboundSyncResult {
        let request = AcknowledgeAlarmRequest {
            devices: vec![format!("{device}#Epics")],
            user: user.to_string(),
        };
        self.execute_command("acknowledge", request).await
    }

    /// Sends the command to the Controls alarms server to bypass the alarming device.
    pub async fn bypass_alarm(&self, device: &str, user: &str) -> OutboundSyncResult {
        let request = BypassAlarmRequest {
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
        let request = SnoozeAlarmRequest {
            devices: vec![format!("{device}#Epics")],
            user: user.to_string(),
            wake: Some(wake),
        };
        self.execute_command("snooze", request).await
    }

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

trait CommandRequest<Request> {
    fn send_request(
        &mut self,
        request: Request,
    ) -> impl std::future::Future<Output = Result<(), tonic::Status>> + Send;
}

impl CommandRequest<AcknowledgeAlarmRequest> for AlarmCommandsClient<Channel> {
    async fn send_request(
        &mut self,
        request: AcknowledgeAlarmRequest,
    ) -> Result<(), tonic::Status> {
        self.acknowledge_alarm(request).await?;
        Ok(())
    }
}

impl CommandRequest<BypassAlarmRequest> for AlarmCommandsClient<Channel> {
    async fn send_request(&mut self, request: BypassAlarmRequest) -> Result<(), tonic::Status> {
        self.bypass_alarm(request).await?;
        Ok(())
    }
}

impl CommandRequest<SnoozeAlarmRequest> for AlarmCommandsClient<Channel> {
    async fn send_request(&mut self, request: SnoozeAlarmRequest) -> Result<(), tonic::Status> {
        self.snooze_alarm(request).await?;
        Ok(())
    }
}

impl Clone for ControlsClient {
    fn clone(&self) -> Self {
        Self {
            connection_manager: Arc::clone(&self.connection_manager),
        }
    }
}
