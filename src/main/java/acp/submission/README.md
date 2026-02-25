
## Run with Docker

Build: docker build -t acp-image-cw1 .

Run: docker run --rm -p 8080:8080\
-e
ACP_POSTGRES="jdbc:postgresql://host.docker.internal:5432/acp?user=postgres&password=postgres"\
-e ACP_S3="http://host.docker.internal:4566"\
-e ACP_DYNAMODB="http://host.docker.internal:4566"\
-e ACP_URL_ENDPOINT="`<external-service-url>`{=html}"\
acp-image-cw1

