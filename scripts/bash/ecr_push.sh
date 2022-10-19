NAME=$USER
ACCOUNT=<YOUR_ACCOUNT_ID>
REGION=<YOUR_REGION>
REPO=mr-container-repo

# Get login credentials for target account
aws ecr get-login-password --region $REGION | docker login --username AWS --password-stdin $ACCOUNT.dkr.ecr.$REGION.amazonaws.com

# Build the container locally with docker
docker build -t $REPO-"$NAME" .

# Tag the model for upload to ECR
docker tag $REPO-"$NAME":latest $ACCOUNT.dkr.ecr.$REGION.amazonaws.com/$REPO-"$NAME":latest

# Push to ECR
docker push $ACCOUNT.dkr.ecr.$REGION.amazonaws.com/$REPO-"$NAME":latest
