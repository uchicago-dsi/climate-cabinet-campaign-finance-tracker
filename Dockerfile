# This is a basic docker image for use in the clinic
# It contains 
FROM jupyter/minimal-notebook:python-3.11

# Swith to root to update and install python dev tools
# Then return to NB_UID user. This is monstly done to demonstrate
# how to do this if additional packages are required.
USER root
RUN apt update
RUN apt install -y python3-pip python3-dev
USER $NB_UID

# Create working directory
WORKDIR /project

# Install Python 3 packages
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
# install project as an editable package
COPY utils ./utils
COPY setup.py .
RUN pip install -e .

CMD ["/bin/bash"]