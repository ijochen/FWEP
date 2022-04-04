
####default code when lambda function is created

# import json

# def lambda_handler(event, context):
#     # TODO implement
#     return {
#         'statusCode': 200,
#         'body': json.dumps('Hello from Lambda!')
#     }

#### test s3 object creation trigger
# def lambda_handler(event, context):
#     #TODO implement
#     print("I'm being triggered")
#     return 'Hello from '


import boto3
def lambda_handler(event, context):
    s3 = boto3.client("s3")

    if event: 
        print("Event : ", event)
        file_obj = event["Records"][0]
        filename = str(file_obj)['s3']['object']['key']
        print("Filename: ", filename)
        fileObj = s3.get_object(Bucket = "srs-bucket", Key=filename)
        print("File Obj", fileObj)
        file_content = fileObj["Body"].read().decode('utf-8')
        print(file_content)

    return 'Thanks for Watching'

