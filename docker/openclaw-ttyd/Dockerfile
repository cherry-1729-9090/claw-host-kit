FROM node:22-bookworm-slim

USER root

ARG TTYD_VERSION=1.7.7
ARG TARGETARCH
ARG OPENCLAW_VERSION=latest

RUN apt-get update \
  && apt-get install -y --no-install-recommends bash ca-certificates curl git \
  && rm -rf /var/lib/apt/lists/* \
  && arch="${TARGETARCH:-$(dpkg --print-architecture)}" \
  && case "${arch}" in \
       amd64|x86_64) ttyd_arch="x86_64" ;; \
       arm64|aarch64) ttyd_arch="aarch64" ;; \
       *) echo "Unsupported arch: ${arch}" >&2; exit 1 ;; \
     esac \
  && curl -fsSL -o /usr/local/bin/ttyd \
       "https://github.com/tsl0922/ttyd/releases/download/${TTYD_VERSION}/ttyd.${ttyd_arch}" \
  && chmod 0755 /usr/local/bin/ttyd

ENV HOME=/home/node
ENV OPENCLAW_STATE_DIR=${HOME}/.openclaw
ENV NPM_CONFIG_PREFIX=${HOME}/.npm-global

ENV OPENCLAW_AUTO_APPROVE_CONTROL_UI_PAIRING=true
ENV OPENCLAW_AUTO_APPROVE_POLL_SECONDS=2

COPY entrypoint.sh /usr/local/bin/entrypoint.sh
RUN chmod 0755 /usr/local/bin/entrypoint.sh

USER node

RUN mkdir -p "${OPENCLAW_STATE_DIR}" "${NPM_CONFIG_PREFIX}" \
  && curl -fsSL https://openclaw.ai/install.sh | bash -s -- --install-method npm --version "${OPENCLAW_VERSION}" --no-onboard --no-prompt \
  && test -x "${NPM_CONFIG_PREFIX}/bin/openclaw"

ENV PATH=${NPM_CONFIG_PREFIX}/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin

WORKDIR ${HOME}

EXPOSE 18789 7681

ENTRYPOINT ["/usr/local/bin/entrypoint.sh"]
