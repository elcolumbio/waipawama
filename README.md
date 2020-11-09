# waipawama
A python datapipeline project which builds on pydantic.

I have some new ideas how i want to evolve my datapipelines.
We will use the chance to use some modern python. Inspired by other great open source projects, e.g. fastapi.

## How to get started
In the process i will learn a lot.
Right now the big picture looks like this:

- Build Pydantic Data Models
- Use them for testing your DataPipeline
- Use Pydantic for the configs

- Build Datapipeline, i want to try out apache-airflow this time
  - SQL
  - Pandas Dataframes
  - NoSQL?

First we want to keep SQL, Pandas and Pydantic Models seperate. Because we want to implement native and fast solutions.


## Ideas
Maybe there is an argument to glue SQL, Pandas and Pydantic Models together. If so are we building a framework?

We will see how things fit together. Possible that we expose our Datapipeline with FastAPI.

## Helpful ressources
- [Data templates with pydantic!](https://ianwhitestone.work/data-templates-with-pydantic/)
- pydantic docs