# Planned merge and integration of starvers, StarVersServer, and starvers_eval

StarVersServer resides in the Github repository: https://github.com/martinmitteregger/StarVersServer


# New Project Structure
Starvers
|-- app
    |-- api/
    |-- enums/
    |-- gui/
    |-- models/
    |-- services/
        |-- __init__.py
        |-- DeltaCalculationService.py
        |-- ManagementService.py
        |-- MetricsService.py
        |-- PollingTask.py
        |-- ScheduledThreadPoolExectuor.py
        |-- VersioningService.py
    |-- utils/
        |-- graphdb/
            |-- GraphDataBaseUtils.py 
            |-- queries/
        |-- RDFValidator/
        |-- __init__.py
        |-- starvers.py
        |-- HelperService.py 
        |-- _helper.py
        |-- _prefixes.py
    |-- exceptions
        |-- DatasetNotFoundException.py
        |-- RepositoryCreationFailedException.py
        |-- ServerFileImportFailedException.py
        |-- _exceptions.py
    |-- __init__.py
    |-- AppConfig.py
    |-- Database.py
    |-- LoggingConfig.py
    |-- main.py
    |-- run_gui.py
|-- assets/
|-- demos/
    |-- starvers_demo_hust.ipynb
|-- docker-dev
|-- docerk-prod
|-- evaluation
    |-- delta_eval
        |-- evaluation.py -> plots.py
        |-- compute.py
    |-- starvers_eval
        |-- output/
        |-- raw_queries/
        |-- experiments
            |-- benchmark
                |-- 1_download
                |-- 2_preprocess_data
                |-- 3_construct_datasets
                |-- 4_ingest
                |-- 5_construct_queries
                |-- 6_evaluate
                |-- 7_visualize
            |-- analysis
            |-- retrieval
            |-- triple_store_mgmt
            |-- eval_setup.toml
            |-- logging.py
            |-- requirements.txt
        |-- .env
        |-- docker-compose.yaml
        |-- Dockerfile
        |-- Dockerfile.admin
|-- tests
    |-- queries/
    |-- Dockerfile
    |-- README.md
    |-- test_insert.py
    |-- test_outdate.py
    |-- test_query.py
    |-- test_update.py
    |-- Testcompute.py
    |-- TestManagementRestService.py
    |-- TestStarVersService.py
    |-- TestStarVersTiming.py
.gitignore
Dockerfile
pytest.ini
README.md
requirements.txt
setup.py
pyproject.toml
TODOs.md
LICENSE

# Questions
Write module names with capital or small letter?

Make exception modules private or not?