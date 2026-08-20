FROM adregistry.fnal.gov/dev-containers/redhat-ubi9-minimal@sha256:ec08129f809d3a00e60e040fef825da752872b561e3a20250f3232209907e130

RUN useradd -u 10001 -r -M -s /sbin/nologin appuser

COPY --chown=10001:10001 target/release/alarms-actions-synchronizer /usr/local/bin/alarms-actions-synchronizer

USER 10001

ENTRYPOINT ["/usr/local/bin/alarms-actions-synchronizer"]