FROM python:3.12
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

RUN apt-get update \
 && apt-get install -y --no-install-recommends \
      gdal-bin \
      libgeos-dev \
      libproj-dev \
      libssl-dev \
      postgresql-server-dev-all \
      python3-gdal \
 && rm -rf /var/lib/apt/lists/*

# CONTEXT_SUBDIR is the path from the build context root to this repo's root.
# For a standalone build (i.e. the context is this repo), leave as "." (the default).
# For a monorepo build where this repo has siblings, set to e.g. "data-management".
ARG CONTEXT_SUBDIR=.

WORKDIR /tmp
COPY ${CONTEXT_SUBDIR}/requirements.txt .
COPY ${CONTEXT_SUBDIR}/plugin-stub ./plugin-stub
RUN pip install --no-cache-dir -r requirements.txt

WORKDIR /src
COPY ${CONTEXT_SUBDIR}/ /src/data-management

# Optionally override the ids-drr-plugin stub with a real implementation.
# PLUGIN_PACKAGE is a path relative to the build context root. The default
# points at the stub inside this repo (already installed above, so the
# reinstall is a no-op). To install a real package in a monorepo layout,
# set e.g. PLUGIN_PACKAGE=ids-drr-plugin (if it is a sibling of this repo).
ARG PLUGIN_PACKAGE=${CONTEXT_SUBDIR}/plugin-stub
COPY ${PLUGIN_PACKAGE} /src/override-plugin
RUN grep -v '^\./' /tmp/requirements.txt > /tmp/constraints.txt \
 && pip install --no-cache-dir -c /tmp/constraints.txt /src/override-plugin \
 && pip install --no-cache-dir --force-reinstall --no-deps /src/override-plugin

WORKDIR /src/data-management
