def lambda_handler(event, context):
    print(f"Second lambda")
    return {"statusCode": 200, "body": "Hello, World!"}
