# Install dependencies
npm install --os=linux --cpu=x64

# AWS Lambda Image Converter and Resizer
zip -r lambda-function.zip .

# Update the Lambda function code
aws lambda update-function-code --function-name convert-and-resize-image --zip-file fileb://lambda-function.zip