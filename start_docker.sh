docker build -t cs/drill_dev_unit_test .
docker run \
  -it \
  -v `pwd`:/drill \
  -w /drill \
  -p 8050:8050 \
  -p 8051:8051 \
  -p 7050:7050 \
  -p 7051:7051 \
  cs/drill_dev_unit_test \
  /bin/bash -c "\
    sh .jenkins/start_kudu.sh && \
    /bin/bash
  "
