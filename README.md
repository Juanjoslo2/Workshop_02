# ETL Project with Airflow and MusicBrainz

This project implements an **ETL** (Extract, Transform, Load) pipeline using **Apache Airflow** to orchestrate tasks and **Python** as the primary programming language. Data is extracted from multiple sources, including the mandatory integration with the **MusicBrainz API** to obtain artist information, processed, and then loaded into a database. Subsequently, reports and visualizations (for example, with **Power BI**) are generated to extract insights from the processed data.

## Objectives

- **Extraction:**  
  - **Datasets:**  
    - **Spotify Dataset:** A CSV file that contains metadata and audio characteristics of songs.  
    - **Grammys Dataset:** The initial database containing information about Grammy nominations and awards.  
  - **MusicBrainz API:** Mandatory additional information is obtained from artists, such as identifiers and other relevant details, through the MusicBrainz API.

- **Transformation:**  
  Perform exploratory analysis, data cleaning, and merging using notebooks and tasks orchestrated with Apache Airflow.

- **Load:**  
  Save the processed information into a database (for example, PostgreSQL) and export CSV files to external services (such as Google Drive) for further analysis.

## Main Technologies and Tools

- **Language:** Python (Python 3.10 or higher is recommended)  
  [Download Python](https://www.python.org/downloads/)

- **Orchestration:** Apache Airflow  
  [Airflow Documentation](https://airflow.apache.org/docs/)

- **Data Handling:** pandas  
  [pandas Documentation](https://pandas.pydata.org/)

- **Database:** PostgreSQL  
  [Download PostgreSQL](https://www.postgresql.org/download/)

- **ORM and Connection:** SQLAlchemy  
  [SQLAlchemy Documentation](https://docs.sqlalchemy.org/)

- **Visualization:** Power BI (Desktop)  
  [Power BI Desktop](https://www.microsoft.com/en-us/power-platform/products/power-bi/desktop)

- **Development Environment:** Jupyter Notebook (used via VS Code)  
  [Jupyter in VS Code Guide](https://code.visualstudio.com/docs/datascience/jupyter-notebooks)

- **Cloud Storage:** Google Drive via PyDrive2  
  [PyDrive2 Documentation](https://docs.iterative.ai/PyDrive2/)

- **API:** MusicBrainz  
  [MusicBrainz API Documentation](https://musicbrainz.org/doc/MusicBrainz_API)

---

## Project Structure

Below is the main folder and file structure of the project:

```
├── airflow/
│   ├── dags/                    # Definition of Airflow DAGs.
│   ├── tasks/                   # Python scripts with specific pipeline tasks.
│   └── (Other Airflow configuration files)
│
├── data/
│   ├── spotify_dataset.csv      # Song data and features from the Spotify dataset.
│   └── the_grammy_awards.csv    # Data on Grammy nominations and awards.
│
├── drive_config/                # Configuration and credentials for Google Drive (PyDrive2).
│
├── notebooks/
│   ├── 01-grammy_raw_load.ipynb   # Notebook to load the Grammys dataset into the DB.
│   ├── 02-EDA_Spotify.ipynb       # Exploratory analysis of the Spotify dataset.
│   ├── 04-EDA_Grammys.ipynb       # Exploratory analysis of the Grammys dataset.
│   └── 05-api_extract.ipynb       # Extraction from the API and conversion to a CSV named musicbrainz.csv to store the extracted information.
│
├── src/ 
│
├── db/
│   ├── db_operations            # Functions related to database connection.
│       
├── extract/
│   ├── api_extract              # Extraction from the MusicBrainz API and cleaning of that data.
│   ├── grammys_extract.py       # Extraction and processing of the Grammys table in the DB.
│   └── spotify_extract.py       # Extraction and processing of the Spotify dataset.
│          
├── load_store/
│   ├── load.py                  # Data loading.
│   ├── .env                     # Contains information regarding the drive configuration: FOLDER_ID, CONFIG_DIR, etc.
│   └── store.py                 # Storage on Google Drive or other services.
│
├── transform/
│   ├── api_transform.py         # Transformations related to the data extracted from the API.
│   ├── grammys_transform.py     # Transformations related to the Grammys table in PostgreSQL.
│   ├── spotify_transform.py     # Transformations related to the Spotify dataset.
│   └── merge.py                 # Merging the three data sources into one after cleaning and transformation.
│ 
├── .gitignore                   # Files and folders excluded from the repository.
├── .env                         # Environment variables (credentials, paths, etc.).
├── requirements.txt             # List of project dependencies.
├── start.sh                     # Running this file sets the Airflow home and starts its implementation.
```

---

## Requirements and Initial Configuration

### 1. Clone the Repository

Run the following command in the terminal:

```bash
git clone https://github.com/Juanjoslo2/Workshop_02.git
cd Workshop_02
```

### 2. Create and Activate the Virtual Environment

It is recommended to isolate dependencies:
```bash
python3 -m venv venv
source venv/bin/activate   # On Windows: venv\Scripts\activate
```

### 3. Install Dependencies

Install the necessary libraries:
```bash
pip install -r requirements.txt
```

### 4. Configure Environment Variables

In the root folder, create a file called `.env` (or inside an `env/` folder) with the following format, adapting it to your configuration:

```ini
# PostgreSQL (Database)
PG_HOST=localhost
PG_PORT=5432
PG_USER=your_user
PG_PASSWORD=your_password
PG_DATABASE=your_db_name

# Google Drive (PyDrive2)
CLIENT_SECRETS_PATH="/path/to/drive_config/client_secrets.json"
SETTINGS_PATH="/path/to/drive_config/settings.yaml"
SAVED_CREDENTIALS_PATH="/path/to/drive_config/saved_credentials.json"
FOLDER_ID=your_folder_id

# MusicBrainz API 
MUSICBRAINZ_USER_AGENT="your_app_name/1.0 ( your_email@domain.com )"
```

---

## Running the Project

### 1. Exploratory Analysis

- `01-raw_Grammys_load.ipynb`: Load the Grammys CSV as a table into the database.  
- `02-spotify_EDA.ipynb`: Exploratory analysis of the Spotify dataset.
- `03-Grammys_EDA.ipynb`: Exploratory analysis of the Grammys dataset.  
- `04_api_extract.ipynb`: Data extraction from the MusicBrainz API.

> **Important:** Make sure to select the appropriate kernel in Jupyter Notebook and verify that the database connection is correctly configured.

### 2. Running the ETL Pipeline

- Confirm that all required libraries are installed; if not, run `pip install -r requirements.txt` again.
- Start the project by running the `start.sh` file:  
  ```bash
  source start.sh
  ```
- This will initialize Airflow, and then in the Airflow web page you can start the DAG to execute the established tasks.
- If the tasks run correctly, a new table called `merged_data` will be created in the database, and a CSV file named `merged_data.csv` will be uploaded to the designated Google Drive folder.

## Data Visualization

To generate dashboards with **Power BI** or another BI tool:

1. Open Power BI Desktop.
2. Connect to the PostgreSQL database using the defined credentials.
3. Select the tables generated by the pipeline.
4. Create charts and dashboards that illustrate the insights obtained from the ETL process.

---

## Documentation and Additional Resources

- [Apache Airflow Documentation](https://airflow.apache.org/docs/)
- [PyDrive2 Guide](https://docs.iterative.ai/PyDrive2/)
- [PostgreSQL Documentation](https://www.postgresql.org/docs/)
- [MusicBrainz API](https://musicbrainz.org/doc/MusicBrainz_API)
- [Power BI Desktop](https://www.microsoft.com/en-us/power-platform/products/power-bi/desktop)

---

## Final Considerations

- **Environment:** It is recommended to run this project in a Linux environment or use WSL on Windows, since Apache Airflow runs optimally on Unix systems.
- **Security:** Ensure that sensitive files, such as the `.env` file and credentials, are kept out of the repository.
- **Updates:** Regularly review and update dependencies and configuration to maintain a robust development environment.

