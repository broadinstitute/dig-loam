# Start from regenie base image
FROM ghcr.io/rgcgithub/regenie/regenie:v3.1.2

SHELL ["/bin/bash", "-lc"]
ENV DEBIAN_FRONTEND=noninteractive

# Install bash + python + pip for dxpy
RUN apt-get update && apt-get install -y --no-install-recommends bash ca-certificates coreutils findutils jq python3 python3-pip locales tabix && \
	ln -s /usr/bin/tabix /usr/local/bin/tabix && \
	ln -s /usr/bin/bgzip /usr/local/bin/bgzip && \
	apt-get clean && \
	rm -rf /var/lib/apt/lists/*

# set the locale
RUN sed -i -e 's/# en_US.UTF-8 UTF-8/en_US.UTF-8 UTF-8/' /etc/locale.gen && locale-gen
ENV LANG=en_US.UTF-8  
ENV LANGUAGE=en_US:en  
ENV LC_ALL=en_US.UTF-8     

# Install dxpy -> /usr/local/bin/dx
ENV PIP_DISABLE_PIP_VERSION_CHECK=1
RUN python3 -m pip install --no-cache-dir --upgrade pip setuptools wheel \
 && python3 -m pip install --no-cache-dir dxpy

# Ensure regenie binary is symlinked to /usr/local/bin/regenie
# (in case the base image puts it somewhere else)
RUN if [ -x /opt/regenie/regenie ]; then \
      ln -sf /opt/regenie/regenie /usr/local/bin/regenie; \
    elif [ -x /regenie ]; then \
      ln -sf /regenie /usr/local/bin/regenie; \
    else \
      echo "WARNING: regenie not found in expected locations; check base image" >&2; \
    fi

# Sanity checks: dx and regenie
RUN which dx && ls -l /usr/local/bin/dx \
 && which regenie && ls -l /usr/local/bin/regenie || true

# Bootstrap wrapper: source DNAnexus job env so dx works in RAP
RUN cat >/usr/local/bin/dx-bootstrap.sh <<'BASH'
#!/bin/bash
set -euo pipefail
if [[ -f /home/dnanexus/environment ]]; then
  # shellcheck source=/dev/null
  source /home/dnanexus/environment || true
fi
export PATH="/usr/local/bin:/opt/dnanexus/dx-toolkit/bin:/home/dnanexus/dx-toolkit/bin:$PATH"
exec "$@"
BASH
RUN chmod +x /usr/local/bin/dx-bootstrap.sh

ENTRYPOINT ["/usr/local/bin/dx-bootstrap.sh", "/bin/bash", "-lc"]
