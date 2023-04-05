FROM python:3.11.2
ADD pyproject.toml /

RUN pip install poetry

RUN poetry config virtualenvs.create false
RUN poetry install

WORKDIR /pipelines-main

COPY poetry.lock pyproject.toml ./

COPY ./db /pipelines-main/db
COPY ./example_pipeline/ /pipelines-main
COPY ./example_pipeline/pipeline.py /pipelines-main/example_pipeline/
COPY ./pipelines /pipelines-main/pipelines
COPY ./README.md /pipelines-main

RUN pip install -e .

CMD ["python", "./example_pipeline/pipeline.py"]
