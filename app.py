import dash
from dash import dcc, html, Input, Output, State
import pymysql
import pandas as pd
import os
import boto3
import base64

# Initialize the Dash app
app = dash.Dash(__name__)
server=app.server
# Define the layout of the app
app.layout = html.Div([
    dcc.Input(id='name', type='text', placeholder='Name'),
    dcc.Input(id='age', type='number', placeholder='Age'),
    dcc.Input(id='position', type='text', placeholder='Position'),
    dcc.Input(id='department', type='text', placeholder='Department'),
    dcc.Input(id='salary', type='number', placeholder='Salary'),
    dcc.Input(id='location', type='text', placeholder='Location'),
    dcc.Upload(id='upload-data', children=html.Button('Upload File')),
    html.Button('Submit', id='submit-button'),
    html.Div(id='output-message')
])

# Define the callback to handle form submission
@app.callback(
    Output('output-message', 'children'),
    Input('submit-button', 'n_clicks'),
    Input('upload-data', 'contents'),
    State('name', 'value'),
    State('age', 'value'),
    State('position', 'value'),
    State('department', 'value'),
    State('salary', 'value'),
    State('location', 'value')
)
def insert_into_database(n_clicks, contents, name, age, position, department, salary, location):
    if n_clicks is None:
        return ''

    # Connect to the RDS MySQL database
    connection = pymysql.connect(
        host='',
        user='',
        password='',
        database=''
    )

    try:
        with connection.cursor() as cursor:
            # Insert data into the database
            sql = "INSERT INTO employees (name, age, position, department, salary, location) VALUES (%s, %s, %s, %s, %s, %s)"
            cursor.execute(sql, (name, age, position, department, salary, location))
        connection.commit()
        uploaded_filename = None
        if contents:
            content_type, content_string = contents.split(',')
            decoded = base64.b64decode(content_string)
            uploaded_filename = 'uploaded_file.csv'
            with open(uploaded_filename, 'wb') as f:
                f.write(decoded)

        if uploaded_filename:
            # Upload the uploaded file to S3
            s3 = boto3.client('s3',
                                aws_access_key_id='',
                                aws_secret_access_key='')

            s3_bucket_name = 'batch1-2-realtime-project1'
            s3_object_key = 'uploaded_file.csv'
            s3.upload_file(uploaded_filename, s3_bucket_name, s3_object_key)

            os.remove(uploaded_filename)  # Remove the uploaded file

        return 'Data inserted into RDS MySQL and file uploaded to S3.'  

    except Exception as e:
        return f'Error: {str(e)}'
    
    

    finally:
        connection.close()