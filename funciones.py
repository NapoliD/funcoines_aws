
def _sqlserver(server,database,username,password):
    import pyodbc
    try:
        return pyodbc.connect('DRIVER={ODBC Driver 17 for SQL Server};SERVER='+server+';DATABASE='+database+';UID='+username+';PWD='+ password )
    except:
        
        return print('conexion faill') 
   
     

def _pymysql(password):
    import pymysql
    try:
        return pymysql.connect(user=user,password=password,host="xxxus-east-1.rds.amazonaws.com",port=3306,database=database)
    except:
        
        return print('conexion faill') 
def save_csv(bucket,nombre_archivo,data):
    from io import StringIO
    import boto3
    csv_buffer = StringIO()
    s3_resource= boto3.resource(service_name = 's3')
    data.to_csv(csv_buffer, encoding='utf8', sep=',', index=False)
    s3_resource.Object(bucket, nombre_archivo).put(Body=csv_buffer.getvalue())
    return print('ok guardar_csv ')    
  
  
  def send_mail(mails,html2,SUBJECT):
    import boto3
    client = boto3.client('ses',region_name = 'us-east-1')
    return client.send_email(Destination={'ToAddresses':mails },Message={'Body': {'Html': {'Data': html2},'Text': {'Charset': 'UTF-8','Data': ''},},'Subject': {'Data': SUBJECT},},Source="@.com")
  
  def save_parquet(bucket_,nombre_archivo_,data):
    from io import BytesIO
    import boto3
    out_buffer = BytesIO()
    s3 = boto3.client("s3")
    data.to_parquet(out_buffer, index=False)
    s3.put_object(Bucket=bucket_, Key=nombre_archivo_, Body=out_buffer.getvalue())
  
  def open_parquet():
    import io
    client = boto3.resource('s3')
    buffer = io.BytesIO()
    object = client.Object(bucket, filename)
    object.download_fileobj(buffer)
    return pd.read_parquet(buffer)

  
  pd.to_datetime('', format='%Y%m%d', errors='coerce')
