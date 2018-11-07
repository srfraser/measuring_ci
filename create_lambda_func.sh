#!/bin/bash

set -e

if [ "$(uname -s)" != 'Linux' ]; then
    echo "Some modules are compiled, please build on Linux"
    echo "Consider using ./create_lambda_func_in_docker.sh"
    exit 1
fi

cd /work || exit 1

STAGING_DIR="$(pwd)/staging"
rm -fr "${STAGING_DIR}"
OUTPUT_FILENAME="$(pwd)/measuring_ci.zip"

mkdir -p "${STAGING_DIR}"

cp -pr "pushlog_scanner.py" "${STAGING_DIR}/"
cp -pr "releases_scanner.py" "${STAGING_DIR}/"
cp -pr "nightly_scanner.py" "${STAGING_DIR}/"

cp -p *.yml "${STAGING_DIR}/"

cp -pr "measuring_ci" "${STAGING_DIR}"

VENV_NAME="venv-$$"
virtualenv -p python3 "${VENV_NAME}"
# shellcheck disable=SC1090
source "${VENV_NAME}/bin/activate"

pip install -r requirements/main.txt
SITE_PACKAGES=$(find ${VENV_NAME} -type d -name site-packages)
# boto is already included in the lambda environment
# plotly/ipython are huge, and there's a 256Mb unzipped size limit
# for the env we upload.

rsync -av --exclude "*boto*" --exclude "*pip*" --exclude "*plotly*" --exclude "*ipython*" --exclude "*jupyter*" --exclude "*/tests/*" "${SITE_PACKAGES}"/* "${STAGING_DIR}/"

# mv  "${SITE_PACKAGES}"/* "${STAGING_DIR}/"
# rm -fr "${STAGING_DIR}"/plotly*
# rm -fr "${STAGING_DIR}"/jupyter*
# rm -fr "${STAGING_DIR}"/ipython*

# Not yet pip installable
git clone https://github.com/srfraser/taskhuddler "${STAGING_DIR}/taskhuddler_stage"
mv "${STAGING_DIR}/taskhuddler_stage/taskhuddler" "${STAGING_DIR}/taskhuddler"
rm -fr "${STAGING_DIR}/taskhuddler_stage"

deactivate

rm -fr "${VENV_NAME}"

rm -f "${OUTPUT_FILENAME}"

(
    cd "${STAGING_DIR}" || exit 1
    zip -r "${OUTPUT_FILENAME}" .
)

rm -fr "${STAGING_DIR}"


echo "Now for some manual steps:"
echo "1. Upload $(basename "${OUTPUT_FILENAME}")"
echo "aws s3 cp measuring_ci.zip s3://mozilla-releng-metrics/$(basename "${OUTPUT_FILENAME}")"
echo ""

echo "2. Visit https://console.aws.amazon.com/lambda/home?region=us-east-1#/functions/measuring_ci_parquet_update"
echo "ARN - arn:aws:lambda:us-east-1:314336048151:function:measuring_ci_parquet_update"
echo "3. Under 'Function code' choose a 'Code entry type' of 'Upload a file from Amazon S3'"
echo "Paste the above s3 url into the box"
echo "4. Ensure the Handler is set correctly if not using lambda_function:lambda_handler()"
echo "5. Under 'Basic Settings' ensure the Memory usage is at 512Mb and Timeout is at least 2 minutes."
echo "6. Click 'Save' at the top of the page"

echo ""
echo "3 Test the lambda function using the 'Test' button. The event itself doesn't matter"
echo "If a test event is not defined, the basic 'Hello world' template will do, as we're not using the event data."
echo ""

echo "All done!"


