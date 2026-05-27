//! Phoebus Synchronization Module
//!
//! Handles how updates from Phoebus are communicated to the Controls alarms server.

use rust_grpc_lib::pool;
use tonic::transport::Channel;
use tracing::{error, info};

use crate::models::OutboundSyncResult;
use crate::models::proto::google::protobuf::Timestamp;
use crate::models::proto::services::alarm_commands::alarm_commands_client::AlarmCommandsClient;
use crate::models::proto::services::alarm_commands::{
    AcknowledgeRequest, ActivateRequest, BypassRequest, SnoozeRequest,
};

#[cfg(test)]
mod tests;

/// Handles the reference to the global [`AlarmCommandsClient`] instance and provides methods to interact
/// with the client to issue commands.
#[derive(Clone)]
pub struct ControlsClient {
    grpc_alarms_svc_host: String,
}

impl ControlsClient {
    /// Creates a new [`ControlsClient`] that will connect to the Controls gRPC alarms service at the provided host.
    pub fn new(grpc_alarms_svc_host: &str) -> Self {
        Self {
            grpc_alarms_svc_host: grpc_alarms_svc_host.to_owned(),
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
        let mut client = match pool::get(&self.grpc_alarms_svc_host) {
            Err(e) => {
                error!("Could not get a connection to the Controls alarms service: {e}");
                return OutboundSyncResult::Failed;
            }
            Ok(c) => c,
        };

        if Self::run_operation(&mut client, request.clone()).await {
            return OutboundSyncResult::Succeeded;
        }

        info!(
            command = command_label,
            "Initial RPC failed. Making one more attempt."
        );

        if Self::run_operation(&mut client, request).await {
            OutboundSyncResult::Succeeded
        } else {
            OutboundSyncResult::Failed
        }
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
