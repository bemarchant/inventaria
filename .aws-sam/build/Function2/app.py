
def lambda_handler(event, context):
    print(f"First lambda")
    return {"statusCode": 200, "body": "Hello, World!"}
