import logging
from typing import List

from airflow.contrib.hooks.aws_hook import AwsHook
from botocore.exceptions import ClientError


class GlueDataCatalogHook(AwsHook):
    def get_conn(self):
        self.conn = self.get_client_type('glue', self.region_name)
        return self.conn

    def get_records(self, sql):
        raise NotImplementedError(f"{__class__}#get_records is not implemented")

    def get_pandas_df(self, sql):
        raise NotImplementedError(f"{__class__}#get_pandas_df is not implemented")

    def run(self, sql):
        raise NotImplementedError(f"{__class__}#run is not implemented")

    def __init__(
            self,
            aws_conn_id: str = 'aws_default',
            region_name: str = None,
            catalog_id: str = None,
            *args,
            **kwargs):
        self.region_name = region_name
        self.catalog_id = catalog_id
        super().__init__(aws_conn_id=aws_conn_id, *args, **kwargs)

    def get_database(self, name: str) -> dict:
        args = {
            'Name': name,
        }
        if self.catalog_id:
            args['CatalogId'] = self.catalog_id
        return self.get_conn().get_database(**args)['Database']

    def does_database_exists(self, name: str) -> bool:
        try:
            self.get_database(name=name)
            return True
        except ClientError as ex:
            # NOTE: cannot import botocore.errorfactory.EntityNotFoundExc
            if ex.__class__.__name__ == 'EntityNotFoundException':
                return False
            raise ex

    def get_table(self, db: str, name: str) -> dict:
        args = {
            'DatabaseName': db,
            'Name': name,
        }
        if self.catalog_id:
            args['CatalogId'] = self.catalog_id
        return self.get_conn().get_table(**args)['Table']

    def does_table_exists(self, db: str, name: str) -> bool:
        try:
            self.get_table(db=db, name=name)
            return True
        except ClientError as ex:
            if ex.__class__.__name__ == 'EntityNotFoundException':
                return False
            raise ex

    def get_partition_keys(self, db: str, name: str) -> List[str]:
        table = self.get_table(db=db, name=name)
        partition_keys = []
        for p in table['PartitionKeys']:
            partition_keys.append(p['Name'])
        return partition_keys

    def get_table_location(self, db: str, name: str) -> str:
        table = self.get_table(db=db, name=name)
        if 'Location' not in table['StorageDescriptor']:
            raise GlueDataCatalogError(f"Table[{db}.{table}] does not have Location")
        return table['StorageDescriptor']['Location']

    def delete_table(self, db: str, name: str) -> None:
        args = {
            'DatabaseName': db,
            'Name': name
        }
        if self.catalog_id:
            args['CatalogId'] = self.catalog_id
        self.get_conn().delete_table(**args)

    def get_partition(self, db: str, table_name: str, partition_values: List[str]) -> dict:
        args = {
            'DatabaseName': db,
            'TableName': table_name,
            'PartitionValues': partition_values
        }
        if self.catalog_id:
            args['CatalogId'] = self.catalog_id
        return self.get_conn().get_partition(**args)['Partition']

    def does_partition_exists(self, db: str, table_name: str, partition_values: List[str]) -> bool:
        try:
            self.get_partition(db=db, table_name=table_name, partition_values=partition_values)
            return True
        except ClientError as ex:
            if ex.__class__.__name__ == 'EntityNotFoundException':
                return False
            raise ex

    def delete_partition(self, db: str, table_name: str, partition_values: List[str]) -> None:
        args = {
            'DatabaseName': db,
            'TableName': table_name,
            'PartitionValues': partition_values
        }
        if self.catalog_id:
            args['CatalogId'] = self.catalog_id
        self.get_conn().delete_partition(**args)

    def create_partition(self, db: str, table_name: str, partition_values: List[str], location: str) -> None:
        table = self.get_table(db=db, name=table_name)
        sd = table['StorageDescriptor']
        args = {
            'DatabaseName': db,
            'TableName': table_name,
            'PartitionInput': {
                'Values': partition_values,
                'StorageDescriptor': {
                    'Location': location,
                    'Columns': sd['Columns'],
                    'InputFormat': sd['InputFormat'],
                    'OutputFormat': sd['OutputFormat'],
                    'Compressed': sd['Compressed'],
                    'SerdeInfo': sd['SerdeInfo'],
                },
            },
        }
        if self.catalog_id:
            args['CatalogId'] = self.catalog_id
        self.get_conn().create_partition(**args)

    def update_partition(self, db: str, table_name: str, partition_values: List[str], location: str) -> None:
        table = self.get_table(db=db, name=table_name)
        sd = table['StorageDescriptor']
        args = {
            'DatabaseName': db,
            'TableName': table_name,
            'PartitionValueList': partition_values,
            'PartitionInput': {
                'Values': partition_values,
                'StorageDescriptor': {
                    'Location': location,
                    'Columns': sd['Columns'],
                    'InputFormat': sd['InputFormat'],
                    'OutputFormat': sd['OutputFormat'],
                    'Compressed': sd['Compressed'],
                    'SerdeInfo': sd['SerdeInfo'],
                },
            },
        }
        if self.catalog_id:
            args['CatalogId'] = self.catalog_id
        self.get_conn().update_partition(**args)


    def convert_table_to_partition(
            self,
            src_db: str, src_table: str,
            dst_db: str, dst_table: str,
            partition_values: List[str]):
        sd = self.get_table(db=src_db, name=src_table)['StorageDescriptor']
        args = {
            'DatabaseName': dst_db,
            'TableName': dst_table,
            'PartitionInput': {
                'Values': partition_values,
                'StorageDescriptor': sd,
            }
        }
        if self.catalog_id:
            args['CatalogId'] = self.catalog_id
        self.get_conn().create_partition(**args)
        self.delete_table(db=src_db, name=src_table)


class Error(Exception):
    pass


class GlueDataCatalogError(Error):
    pass

