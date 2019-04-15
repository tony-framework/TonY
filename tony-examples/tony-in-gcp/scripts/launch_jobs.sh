#!/bin/bash
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS-IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
# This script launches multiple TonY jobs on a master node within a Google Cloud Dataproc cluster.

export CLUSTER_NAME="tony-staging-1"
export BUCKET="tony-staging"
export TONY_JARFILE="gs://${BUCKET}/tony-cli-0.3.1-all.jar"
export TONY_CLASS="com.linkedin.tony.cli.ClusterSubmitter"
export LC_CTYPE=C


function err() {
  echo "[$(date +'%Y-%m-%dT%H:%M:%S%z')]: $@" >&2
  return 1
}

function get_job() {
  local job_id=`head /dev/urandom | tr -dc A-Z0-9 | head -c 6 ; echo ''`
  echo $job_id
}

function execute_tensorflow() {
  # Launch a TensorFlow Job
  if [[ $1 -gt 1 ]]; then
    steps=750
    sleep_timer=15
  else
    steps=1500
    sleep_timer=1
  fi
  echo "Launching ${1} TensorFlow jobs"
  for ((i=1;i<=$1;i++))
  do
    # Generates a random job identifier
    job_id=$(get_job)
    echo "Launching Job: $job_id"
    # Executes the following code inside container:
    # [tf] src_dir $python mnist_distributed.py --task_params ...
    gcloud dataproc --quiet jobs submit hadoop --cluster "${CLUSTER_NAME}" \
    --class com.linkedin.tony.cli.ClusterSubmitter \
    --jars "${TONY_JARFILE}" -- \
    --python_venv=/opt/tony/TonY-samples/deps/tf.zip \
    --python_binary_path=tf/bin/python3.5 \
    --executes mnist_distributed.py \
    --task_params="--steps "${steps}" --data_dir gs://tony-staging/tensorflow/data --working_dir gs://tony-staging/tensorflow/jobs/"${job_id}"/model" \
    --src_dir=/opt/tony/TonY-samples/jobs/TFJob/src \
    --conf_file=/opt/tony/TonY-samples/jobs/TFJob/tony.xml &
    sleep 2
  done
}


function execute_pytorch() {
  # Launch PyTorch MNIST
  echo "Launching ${1} PyTorch jobs."
  for ((i=1;i<=$1;i++))
  do
    # Generates a random job identifier
    job_id=$(get_job)
    echo "Launching Job: $job_id"

    # Executes the following code inside container:
    # [pytorch] src_dir $python3.5 mnist_distributed.py --task_params ...

    gcloud dataproc --quiet jobs submit hadoop --cluster "${CLUSTER_NAME}" \
    --class com.linkedin.tony.cli.ClusterSubmitter \
    --jars "${TONY_JARFILE}" -- \
    --python_venv=/opt/tony/TonY-samples/deps/pytorch.zip \
    --python_binary_path=pytorch/bin/python3.5 \
    --executes mnist_distributed.py \
    --task_params="--root gs://"${BUCKET}"/pytorch/jobs/"${job_id}"" \
    --src_dir=/opt/tony/TonY-samples/jobs/PTJob/src \
    --conf_file=/opt/tony/TonY-samples/jobs/PTJob/tony.xml &
    sleep 2
  done
}


function executes_keras() {
  # Launch Keras MNIST
  echo "Launching ${1} Keras jobs."
  for ((i=1;i<=$1;i++))
  do
    job_id=`head /dev/urandom | tr -dc A-Z0-9 | head -c 6 ; echo ''`
    echo "Launching Job: $job_id"
    gcloud dataproc jobs submit hadoop --cluster "${CLUSTER_NAME}" \
    --class "${TONY_CLASS}" \
    --jars "${TONY_JARFILE}" -- \
    --executes mnist_keras_distributed.py \
    --task_params="--working-dir gs://"${BUCKET}"/tensorflow/jobs/"${job_id}"/model" \
    --src_dir=/opt/tony/TonY-samples/jobs/TFJob/src \
    --conf_file=/opt/tony/TonY-samples/jobs/TFJob/tony.xml \
    --python_venv=/opt/tony/TonY-samples/deps/tf.zip \
    --python_binary_path=tf/bin/python3.5 &
    sleep 2
  done
}

function main() {
  local framework
  local number_of_jobs
  framework="${1}"
  number_of_jobs="${2}"
  # Execute jobs
  echo "Executing job in Dataproc Cluster: $CLUSTER_NAME"
  if [[ "$framework" == 'tensorflow' ]]; then
    execute_tensorflow "${number_of_jobs}" || err "Executing Tensorflow failed"
  elif [[ "$framework" == 'pytorch' ]]; then
  	execute_pytorch "${number_of_jobs}" || err "Executing PyTorch failed"
  elif [[ "$framework" == 'keras' ]]; then
  	executes_keras "${number_of_jobs}" || err "Executing Keras failed"
  else
    return 0
  fi
}

main $1 $2
