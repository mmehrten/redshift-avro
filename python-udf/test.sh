VERSION=2.7
docker run \
    -v "$PWD":/var/task \
    "public.ecr.aws/sam/build-python${VERSION}" \
    "/bin/bash" "-c" "pip install pytest; pytest test_python27.py"