## Requirements

- Docker and Docker Compose
- OpenExchangeRates API key (sign up for free at [openexchangerates.org](https://openexchangerates.org/))

## Project Structure

```
├── dags/                  # Airflow DAG definitions
│   ├── initialize_tables.py      # Creates required database tables
│   ├── orders_data_pipeline.py   # Main data pipeline logic
│   └── setup_connections.py      # Sets up database connections
├── docker-compose.yaml    # Docker configuration
├── Dockerfile             # Custom Airflow image with dependencies
├── requirements.txt       # Python dependencies
└── .env                   # Environment variables (create this file)
```

## Setup Instructions

### 1. Create .env File

Create a `.env` file based on example

### 2. Build and Start the Environment

```bash
docker-compose up -d
```

### 3. Access Airflow

Open your browser and go to http://localhost:8080

- Username: `admin`
- Password: `admin`

### 4. Set Airflow Variables

Go to Admin → Variables in the Airflow UI
Click the "+" button to add a new variable or import all at once

## Stopping the Environment

To stop and remove all containers:

```bash
docker-compose down
```

To also remove the volumes (will delete all data):

```bash
docker-compose down -v
```