//! Phoebus Synchronization Module
//!
//! Handles how updates from Phoebus are communicated to the Controls alarms server.

use crate::models::alarm::{AcknowledgeAlarmRequest, alarm_commands_client::AlarmCommandsClient};
use std::sync::Arc;
use tokio::sync::RwLock;
use tonic::{Response, transport::Channel};
use tracing::error;

/// Shorthand for the shared reference to an optional instance of [`AlarmCommandsClient`].
type ControlsClientConnection = Arc<RwLock<Option<AlarmCommandsClient<Channel>>>>;

/// The logic for passing the provided [`AcknowledgeAlarmRequest`] to an instance of [`AlarmCommandsClient`].
/// Can be passed to [`ControlsClient::send_command`] as the `update` parameter.
async fn acknowledge(
    conn: &mut AlarmCommandsClient<Channel>,
    request: AcknowledgeAlarmRequest,
) -> Result<tonic::Response<()>, tonic::Status> {
    conn.acknowledge_alarm(request).await
}

/// Handles the reference to the global [`AlarmCommandsClient`] instance and provides methods to interact
/// with the client to issue commands.
pub struct ControlsClient {
    connection: ControlsClientConnection,
    grpc_alarms_svc_host: String,
}
impl ControlsClient {
    pub fn new(grpc_alarms_svc_host: &str) -> Self {
        ControlsClient {
            connection: Arc::new(RwLock::new(None)),
            grpc_alarms_svc_host: grpc_alarms_svc_host.to_owned(),
        }
    }

    /// Sends the command to the Controls alarms server to acknowledge the alarming device.
    pub async fn acknowledge_alarm(&self, device: &str, user: &str) {
        let request = AcknowledgeAlarmRequest {
            devices: vec![format!("{device}#Epics")],
            user: user.to_string(),
        };
        self.send_command(acknowledge, request).await
    }

    /// Helper function to acquire a lock on the shared client instance and invoke the provided request.
    /// Clears the reference if there is a problem sending the command, or acquires a new reference if
    /// no existing connection is present.
    async fn send_command<Request, Update>(&self, mut update: Update, request: Request)
    where
        Update: AsyncFnMut(
            &mut AlarmCommandsClient<Channel>,
            Request,
        ) -> Result<Response<()>, tonic::Status>,
    {
        let mut lock = self.connection.write().await;
        match lock.as_mut() {
            Some(conn) => {
                if let Err(e) = update(conn, request).await {
                    error!("Failed to send command to Controls alarms service: {e}");
                    *lock = None;
                }
            }
            None => {
                let mut conn_request =
                    AlarmCommandsClient::connect(self.grpc_alarms_svc_host.clone()).await;
                match conn_request.as_mut() {
                    Ok(conn) => match update(conn, request).await {
                        Ok(_) => *lock = conn_request.ok(),
                        Err(e) => error!("Failed to send command to Controls alarms service: {e}"),
                    },
                    Err(e) => error!("Failed to send command to Controls alarms service: {e}"),
                }
            }
        }
    }
}
impl Clone for ControlsClient {
    fn clone(&self) -> Self {
        ControlsClient {
            connection: Arc::clone(&self.connection),
            grpc_alarms_svc_host: self.grpc_alarms_svc_host.clone(),
        }
    }
}
