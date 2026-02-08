FROM python:3.12-slim

# Install uv
COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /bin/

WORKDIR /app

# Install dependencies globally in the container for simplicity
COPY requirements.txt .
RUN uv pip install --system -r requirements.txt

# Copy relevant data
COPY 01_data_engine/amlsim/outputs/aml_hackathon_v1/ 01_data_engine/amlsim/outputs/aml_hackathon_v1/
COPY 01_data_engine/amlsim/tmp/aml_hackathon_v1/ 01_data_engine/amlsim/tmp/aml_hackathon_v1/

# Copy the data pipeline scripts (Producer/Consumer (Pub/Sub))
COPY 01_data_engine/data_pipeline/ 01_data_engine/data_pipeline/

# Final default command
CMD ["/bin/bash"]