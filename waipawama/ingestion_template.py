from pydantic import BaseModel as PydanticBaseModel
from pydantic import validator
import pandas as pd
from math import isnan
import google
from google.cloud import bigquery
from google.cloud import bigquery_storage
from google.cloud.exceptions import NotFound
from airflow.exceptions import AirflowFailException


class BaseModel(PydanticBaseModel):
    """Cast np.nan from pandas to None and potential ValidationError."""
    @validator('*', pre=True)
    def change_nan_to_none(cls, v, field):
        if type(v) is float and isnan(v):
            return None
        return v


class Meta(PydanticBaseModel):
    """Wrapper with basic function to prepare and push data to bigquery."""
    @property
    def DataFileExists(self):
        if self.data_file.exists():
            return True
        else:
            raise AirflowFailException(f'No such file available: {str(self.data_file)}.')

    def validate(self):
        df = self.read_csv()
        df['Timespan'] = self.timespan
        df['TimeInsert'] = pd.Timestamp.now(tz='utc')
        return df

    def rename_columns(self):
        # dynamic parsing from our model, we could do more things like that.
        rename_columns = {
            model.alias: field for field, model in self.model.__fields__.items()}

        df = self.validate()
        # ignore missing columns because alias can deprecate
        df.rename(rename_columns, axis=1, inplace=True, errors='ignore')
        return df

    def save_as_parquet(self):
        dfrenamed = self.rename_columns()
        dfrenamed.dropna(axis=1, how='all', inplace=True)  # if all values in a column are none
        dfrenamed.to_parquet(self.tmp_file, index=False)
        return True

    def read_parquet(self):
        return pd.read_parquet(self.tmp_file)

    @property
    def TableExists(self):
        client = bigquery.Client()
        try:
            client.get_table(self.table_id)  # Make an API request.
            return True
        except NotFound:
            return False

    def create_schema(self, pydantic_schema):
        """The pydantic schema you get like this: self.model.schema(by_alias=False) ."""
        required = pydantic_schema['required']
        schema = []
        for column, model in pydantic_schema['properties'].items():
            if column in required:
                mode = 'REQUIRED'
            else:
                mode = 'NULLABLE'
            dtype = model['type'].upper()
            if dtype == 'NUMBER':
                dtype = 'FLOAT'
            elif model.get('format') == 'date' or model.get('format') == 'date-time':
                dtype = 'TIMESTAMP'

            description = model.get('description', None)
            schema.append(bigquery.SchemaField(
                column, dtype, mode=mode, description=description))
        return schema

    def create_table(self):
        """Create schema from pydantic model and push."""
        client = bigquery.Client()
        schema = self.create_schema(self.model.schema(by_alias=False))
        table = bigquery.Table(self.table_id, schema=schema)
        table = client.create_table(table)
        return table

    def update_table(self):
        """Create schema from pydantic model, bigquery only allows some changes."""
        client = bigquery.Client()
        schema = self.create_schema(self.model.schema(by_alias=False))
        table = bigquery.Table(self.table_id, schema=schema)
        table = client.update_table(table, ['schema'])
        return table

    def append_data(self):
        """Append data and update schema."""
        client = bigquery.Client()
        job_config = bigquery.LoadJobConfig()
        job_config.write_disposition = bigquery.WriteDisposition.WRITE_APPEND
        # most powerful schema update
        job_config.schema_update_options = [
            bigquery.SchemaUpdateOption.ALLOW_FIELD_ADDITION,
            bigquery.SchemaUpdateOption.ALLOW_FIELD_RELAXATION]
        job_config.schema = self.create_schema(self.model.schema(by_alias=False))
        job_config.source_format = bigquery.SourceFormat.PARQUET

        with open(self.tmp_file, "rb") as source_file:
            job = client.load_table_from_file(
                source_file,
                self.table_id,
                job_config=job_config,)
        return job.result()

    def query_data(self):
        """Query the complete view we just created."""
        # we save 1 call, recommended in docs
        credentials, _ = google.auth.default(
            scopes=["https://www.googleapis.com/auth/cloud-platform"])
        client = bigquery.Client(credentials=credentials)
        bqstorageclient = bigquery_storage.BigQueryReadClient(credentials=credentials)
        # We have the same alias in dbt. E.g. Bankkonto201801
        view = f"{self.table_id.split('.')[-1]}{self.timespan.replace('-','')}"
        df = (
            client.query(f'select * from waipawama.accountant.{view}')
            .result()
            .to_dataframe(bqstorage_client=bqstorageclient))
        return df

    def write_target(self):
        df = self.query_data()
        df.to_csv(
            self.target_file,
            index=False,
            encoding='latin-1',
            sep=';',
            decimal=',',
            date_format='%d.%m.%Y')
        return True
