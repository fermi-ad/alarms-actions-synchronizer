FROM debian:trixie-slim

WORKDIR /app
COPY target/release/alarms-actions-synchronizer /app/alarms-actions-synchronizer
CMD [ "./alarms-actions-synchronizer" ]