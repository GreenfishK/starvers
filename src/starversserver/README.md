# StarVersServer
Command to Run the FastAPI

STEP1: Create virtual environment

python3 -m venv fastapi-env
source fastapi-env/bin/activate

STEP2: Install the requirements inside the virtual environment

pip3 install -r requirements.txt

STEP3: To Run the project

uvicorn app.main:app --reload

STEP4: Swagger UI available in Below path after the app is started successfully

http://127.0.0.1:8000/docs