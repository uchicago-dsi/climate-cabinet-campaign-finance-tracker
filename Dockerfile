FROM jupyter/minimal-notebook:python-3.11

USER root
RUN apt update && apt install -y python3-pip python3-dev

WORKDIR /project

# Adjust permissions for everything within /project before switching back to $NB_UID
COPY --chown=$NB_UID:$NB_GID src ./src
COPY --chown=$NB_UID:$NB_GID pyproject.toml .
COPY --chown=$NB_UID:$NB_GID requirements.txt .

# Switch back to the non-root user before installing Python packages
USER $NB_UID

RUN python -m pip install --upgrade pip && \
    pip install --no-cache-dir -r requirements.txt && \
    pip install -e .

CMD ["/bin/bash"]
