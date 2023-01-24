FROM python:3.10.9

COPY ./requirements.txt /app/requirements.txt

WORKDIR /app

RUN pip install -r requirements.txt

COPY . /app

CMD [ "python", "-m" , "flask", "--app", "src/processor.py", "run", "--host=0.0.0.0"]