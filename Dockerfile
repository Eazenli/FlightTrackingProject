FROM python:3.13-slim

# Set working directory
WORKDIR /app

# Install curl and Poetry
RUN apt-get update && apt-get install -y curl && rm -rf /var/lib/apt/lists/* \
    && pip install --no-cache-dir poetry

# Copy dependency files
COPY pyproject.toml poetry.lock ./

# Install dependencies (no vitralenv)
RUN poetry config virtualenvs.create false \
    && poetry install --no-interaction --no-ansi --no-root 

# Copy project files into /app
COPY . .

# Expose Streamlit port 
EXPOSE 8501

# Health check for Streamlit container
HEALTHCHECK CMD curl --fail http://localhost:8501/_stcore/health || exit 1

