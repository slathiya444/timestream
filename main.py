import boto3
import time
import json

client = boto3.client('timestream-write')
query_client = boto3.client('timestream-query')

if __name__ == "__main__":


    del_table_response = client.delete_table(
        DatabaseName='DemoDB',
        TableName='DemoTable'
    )

    del_response = client.delete_database(
        DatabaseName="DemoDB"
    )

    db_response = client.create_database(
        DatabaseName="DemoDB",
        Tags=[
            {
                'Key': 'string',
                'Value': 'string'
            }
        ]
    )
    # print(db_response)

    tbl_response = client.create_table(
        DatabaseName="DemoDB",
        TableName="DemoTable"
    )

    # print(tbl_response)

    write_records=response = client.write_records(
        DatabaseName='DemoDB',
        TableName='DemoTable',
        Records=[
            {
                'Dimensions': [
                    {
                        'Name': 'id',
                        'Value': '1'
                    },
                    {
                        'Name': 'name',
                        'Value': 'Daniel'
                    },
                ],
                'MeasureName': 'weight',
                'MeasureValue': '48',
                'MeasureValueType': 'DOUBLE',
                'Time': str(int(round(time.time() * 1000))),
                'TimeUnit': 'MILLISECONDS'
            },
            {
                'Dimensions': [
                    {
                        'Name': 'id',
                        'Value': '2'
                    },
                    {
                        'Name': 'name',
                        'Value': 'Sagar'
                    },
                ],
                'MeasureName': 'weight',
                'MeasureValue': '49',
                'MeasureValueType': 'DOUBLE',
                'Time': str(int(round(time.time() * 1000))),
                'TimeUnit': 'MILLISECONDS'
            },
        ]
    )

    query_response = query_client.query(
        QueryString=f"SELECT * FROM {db_response['Database']['DatabaseName']}.{tbl_response['Table']['TableName']}"
    )

    print(json.dumps(query_response, indent=3))