FROM adregistry.fnal.gov/dev-containers/redhat-ubi9-minimal

ARG USER=runner
ENV HOME=/home/$USER

RUN useradd --system --create-home --home-dir $HOME --shell /sbin/nologin $USER
USER $USER

WORKDIR $HOME
COPY --chown=$USER:$USER target/release/alarms-actions-synchronizer $HOME/alarms-actions-synchronizer
CMD [ "./alarms-actions-synchronizer" ]