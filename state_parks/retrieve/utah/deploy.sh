FUNCTION_NAME="UT_state_parks_retrieve"
DEPLOYMENT_FILE="$(pwd)/tmp/${FUNCTION_NAME}_$(date +%Y-%m-%d_%H%M).zip"
CLEAN_FIRST=${CLEAN_FIRST:-"True"}
ASK_BEFORE_DEPLOY=${ASK_BEFORE_DEPLOY:="True"}


echo "Deploy updates to the Lambda function (${FUNCTION_NAME}) to AWS"
echo "Build local zip file with code and dependicies..."

echo ""
echo "  Make tmp folder (might already exists)..."
mkdir -p tmp

if [ "${CLEAN_FIRST}" == "True" ]; then
    echo ""
    rm -rf tmp/build
fi

mkdir -p tmp/build
pip3 install -r requirements.txt -t tmp/build/
cp *.py tmp/build/
pushd tmp/build
zip -q -r9 ${DEPLOYMENT_FILE} *
popd 

echo ""
echo "About to send this file (updated Lambda) to AWS:"
echo ""
ls -lh ${DEPLOYMENT_FILE}
echo ""

if [ "${ASK_BEFORE_DEPLOY}" == "True" ]; then
    read -p "Continue? (Y/N): " confirm && [[ $confirm == [yY] || $confirm == [yY][eE][sS] ]] || exit 1
fi

echo ""
echo "Right now we are always updating the \$LATEST version"
echo "Eventually we may want to use \"--publish\" to also increment the version"
echo ""

echo "Updating \$LATEST version of Lambda (${FUNCTION_NAME}) in AWS..."
aws lambda update-function-code \
    --function-name ${FUNCTION_NAME} \
    --publish \
    --zip-file fileb://${DEPLOYMENT_FILE}

echo ""
echo "Done"
