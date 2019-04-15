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
# This script creates a new Dataproc cluster with 2 nodes, TonY, GPU support and autoscaling.
set -x -e

# Cluster configuration
readonly CLUSTER_NAME='tony-staging-1'
readonly DATAPROC_VERSION='1.3-deb9' # If using a more version of Dataproc verify Python version when executing job.
readonly ZONE='us-central1-b'
readonly BUCKET='tony-staging'

# Initialization actions
readonly TONY_INITIALIZATION_ACTION='gs://tony-staging/initializations/tony_latest.sh'
readonly GPU_INITIALIZATION_ACTION='gs://tony-staging/initializations/install_gpu_cu10.sh'

# Autoscaling name
readonly AUTOSCALE_POLICY='autoscale_tony'

# Metadata
readonly METADATA='tf_version=1.13.1,tf_gpu=true,install_gpu_agent=true'

function err() {
  echo "[$(date +'%Y-%m-%dT%H:%M:%S%z')]: $@" >&2
  return 1
}


function create_autoscaling() {
	# Enable Autoscaling and creates a YAML file
	cat << EOF > autoscale_tony.yaml
basicAlgorithm:
  cooldownPeriod: 120s
  yarnConfig:
    gracefulDecommissionTimeout: 900s
    scaleDownFactor: 1.0
    scaleUpFactor: 1.0
secondaryWorkerConfig:
  weight: 50
workerConfig:
  maxInstances: 10
  minInstances: 2
  weight: 50
EOF
	# Autoscaling policy creation
	gcloud beta dataproc autoscaling-policies import "${AUTOSCALE_POLICY}" --source=autoscale_tony.yaml
}


function create_cluster() {
	# Creates cluster with 4 instances and DominantResourceCalculator enabled
	# Installs TensorFlow with GPU support and NVIDIA drivers in workers.
	create_autoscaling || err "Creation of autoscaling-policy failed"

	gcloud beta dataproc clusters create "${CLUSTER_NAME}" \
	--zone $ZONE \
	--master-machine-type n1-standard-4 --master-boot-disk-size 100 \
	--num-workers 2 --worker-machine-type n1-standard-4 --worker-boot-disk-size 200 \
	--image-version "${DATAPROC_VERSION}" \
	--initialization-actions "${TONY_INITIALIZATION_ACTION}","${GPU_INITIALIZATION_ACTION}" \
	--initialization-action-timeout 20m \
	--scopes https://www.googleapis.com/auth/cloud-platform,https://www.googleapis.com/auth/monitoring.write \
	--autoscaling-policy="${AUTOSCALE_POLICY}" \
	--worker-accelerator type=nvidia-tesla-v100,count=1 \
	--metadata "${METADATA}" \
	--properties "\
capacity-scheduler:yarn.scheduler.capacity.resource-calculator=org.apache.hadoop.yarn.util.resource.DominantResourceCalculator,\
yarn:yarn.log-aggregation-enable=true,\
yarn:yarn.log-aggregation.retain-seconds=-1,\
yarn:yarn.nodemanager.remote-app-log-dir=gs://${BUCKET}/logs,\
yarn:yarn.resourcemanager.webapp.methods-allowed=ALL"

}


function main() {
  # Determine the role of this node
  create_cluster || err "Creation of cluster failed"
}

main