#!/bin/bash -eu
#
# Copyright 2019 Google Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
# This script installs CUDA drivers, CUDNN and NCCL.
set -x -e

# Download settings
readonly REQUIREMENTS_URL='https://raw.githubusercontent.com/GoogleCloudPlatform/dataproc-initialization-actions/master/gpu/gpu_utilization_metrics/requirements.txt'
readonly REPORT_GPU_URL='https://raw.githubusercontent.com/GoogleCloudPlatform/dataproc-initialization-actions/master/gpu/gpu_utilization_metrics/report_gpu_metrics.py'
readonly BUCKET='tony-staging'

systemctl disable unattended-upgrades.service
systemctl disable apt-daily-upgrade.timer
systemctl disable apt-daily.timer

# Creating NVIDIA home dir
export ENV_FILE="/etc/profile.d/env.sh"
cat << 'EOF' > "$ENV_FILE"
export NVIDIA_PATH="/opt/nvidia"
export VERSION_FILE_PATH="$NVIDIA_PATH/version"
export IMAGE_VERSION="cu90"
EOF
source "$ENV_FILE"
mkdir -p "${NVIDIA_PATH}"

echo "${IMAGE_VERSION}" > "${VERSION_FILE_PATH}"


# Error function
function err() {
  echo "[$(date +'%Y-%m-%dT%H:%M:%S%z')]: $@" >&2
  return 1
}


function install_dependencies() {
 # /snap/bin contains gcloud + gsutil, which are used to push/pull artifacts
 # in downstream builds. It must be added to the PATH this way since packer
 # shells are non-interactive
 sed -i 's/^PATH="\(.*\)"/PATH="\1:\/snap\/bin"/g' /etc/environment
 # for this script, we have to manually add it to path
 export PATH=$PATH:/snap/bin

 # security updates
 export DEBIAN_FRONTEND=noninteractive
 apt update
 apt -y upgrade

 # core packages
 apt install -y \
  build-essential \
  software-properties-common \
  git \
  emacs \
  vim \
  curl \
  wget \
  rsync \
  zip \
  unzip \
  tmux \
  screen \
  ca-certificates \
  apt-transport-https \
  pkg-config \
  zlib1g-dev \
  libhdf5-dev \
  libfreetype6-dev \
  libjpeg-dev \
  libpng-dev \
  dkms \
  libopenblas-base \
  pciutils \
  htop \
  protobuf-compiler \
  tree \
  python3-pip python-pip
}


# Write an installer and an uninstaller script to /opt/nvidia/
function driver_installation() {
 source /etc/profile.d/env.sh
 export DEBIAN_FRONTEND=noninteractive
 cat << 'EOF' > "${NVIDIA_PATH}/install-driver.sh"
#!/bin/bash -eu
export DRIVER_INSTALLER_FILE_NAME="driver_installer.run"
wget -q http://us.download.nvidia.com/tesla/390.46/NVIDIA-Linux-x86_64-390.46.run -O ${DRIVER_INSTALLER_FILE_NAME}
chmod +x ${DRIVER_INSTALLER_FILE_NAME}
./${DRIVER_INSTALLER_FILE_NAME} --dkms -a -s --install-libglvnd --no-drm
rm -rf ${DRIVER_INSTALLER_FILE_NAME}
exit 0
EOF
 cat << 'EOF' > "${NVIDIA_PATH}/uninstall-driver.sh"
#!/bin/bash -eu
sudo /usr/bin/nvidia-uninstall --silent
EOF
 chmod +x "${NVIDIA_PATH}/install-driver.sh"
 chmod +x "${NVIDIA_PATH}/uninstall-driver.sh"
 "${NVIDIA_PATH}/install-driver.sh"
}


# Download and install CUDA
function cuda_installer() {
 export CUDA_INSTALLER_FILE_NAME="cuda_installer"
 cd /tmp/
 wget -q https://developer.nvidia.com/compute/cuda/9.0/Prod/local_installers/cuda_9.0.176_384.81_linux-run -O $CUDA_INSTALLER_FILE_NAME
 chmod +x $CUDA_INSTALLER_FILE_NAME
 ./$CUDA_INSTALLER_FILE_NAME --toolkit --override --silent
 echo "/usr/local/cuda/lib64" >> /etc/ld.so.conf.d/nvidia.conf
 rm -rf $CUDA_INSTALLER_FILE_NAME
 ldconfig
}


# CUDA Patches
function download_and_install_patch() {
 cd /tmp/
 patch_file_name="patch.sh"
 rm -rf $patch_file_name
 url=$1
 wget "$1" -O "$patch_file_name"
 chmod +x $patch_file_name
 ./$patch_file_name --accept-eula --silent
}


# Download and install CuDNN
function cudnn_installer() {
 export CUDNN_INSTALLER_FILE_NAME="cudnn_installer"
 cd /tmp/
 gsutil cp gs://"${BUCKET}"/nvidia-drivers/cudnn-9.0-linux-x64-v7.1.tgz ./$CUDNN_INSTALLER_FILE_NAME
 tar -xvzf ./$CUDNN_INSTALLER_FILE_NAME
 cp -r ./cuda/* /usr/local/cuda
 rm -rf $CUDNN_INSTALLER_FILE_NAME
}


# Download and install NCCL
function nccl_installer() {
 export NCCL_DIR=/usr/local/nccl2
 export NCCL_INSTALLER="nccl_installer"
 cd /tmp/
 mkdir -p $NCCL_DIR
 gsutil cp gs://"${BUCKET}"/nvidia-drivers/nccl_2.3.7-1+cuda9.0_x86_64.txz ./nccl_2.3.7-1+cuda9.0_x86_64.txz
 xz -d nccl_2.3.7-1+cuda9.0_x86_64.txz
 tar xvf nccl_2.3.7-1+cuda9.0_x86_64.tar
 cp -r nccl_2.3.7-1+cuda9.0_x86_64/* $NCCL_DIR
 rm -rf nccl_2.3.7-1+cuda9.0_x86_64.tar
 echo "/usr/local/nccl2/lib" >> /etc/ld.so.conf.d/nvidia.conf
 ldconfig
}

# Add CUDA environment variables
function cuda_enviroment() {
 cat << 'EOF' > /etc/profile.d/nvidia-env.sh
export PATH=/usr/local/cuda/bin${PATH:+:${PATH}}
export LD_LIBRARY_PATH=/usr/local/cuda/lib64:/usr/local/nccl2/lib:/usr/local/cuda/extras/CUPTI/lib64${LD_LIBRARY_PATH:+:${LD_LIBRARY_PATH}}
export CUDA_HOME=/usr/local/cuda
EOF
 source /etc/profile.d/nvidia-env.sh
}

# Collect gpu_utilization and gpu_memory_utilization
function install_gpu_agent_service(){
 apt-get install python-pip -y
 wget -O report_gpu_metrics.py "${REPORT_GPU_URL}"
 wget -O requirements.txt "${REQUIREMENTS_URL}"
 cp ./requirements.txt /root/requirements.txt
 cp ./report_gpu_metrics.py /root/report_gpu_metrics.py
 pip install -r ./requirements.txt

 # Generate GPU service.
 cat <<-EOF > /lib/systemd/system/gpu_utilization_agent.service
[Unit]
Description=GPU Utilization Metric Agent
[Service]
Type=simple
PIDFile=/run/gpu_agent.pid
ExecStart=/bin/bash --login -c '/usr/bin/python /root/report_gpu_metrics.py'
User=root
Group=root
WorkingDirectory=/
Restart=always
[Install]
WantedBy=multi-user.target
EOF
 # Reload systemd manager configuration
 systemctl daemon-reload
 # Enable gpu_utilization_agent service
 systemctl --no-reload --now enable /lib/systemd/system/gpu_utilization_agent.service
}

function main() {
  # Determine the role of this node
  local role
  role="$(/usr/share/google/get_metadata_value attributes/dataproc-role)"
  # Only run on the Worker node of the cluster
  if [[ "${role}" == 'Worker' ]]; then
  	install_dependencies || err "Unable to install dependencies"
  	driver_installation || err "Unable to install GPU driver"
  	cuda_installer || err "Unable to install CUDA driver"
  	# Install patches
  	download_and_install_patch https://developer.nvidia.com/compute/cuda/9.0/Prod/patches/1/cuda_9.0.176.1_linux-run
	download_and_install_patch https://developer.nvidia.com/compute/cuda/9.0/Prod/patches/2/cuda_9.0.176.2_linux-run
	download_and_install_patch https://developer.nvidia.com/compute/cuda/9.0/Prod/patches/3/cuda_9.0.176.3_linux-run
	download_and_install_patch https://developer.nvidia.com/compute/cuda/9.0/Prod/patches/4/cuda_9.0.176.4_linux-run
	cudnn_installer || err "Unable to install CuDNN"
	nccl_installer || err "Unable to install NCCL"
	cuda_enviroment || err "Unable to install CUDA environment"

  	# Add licenses
	wget https://docs.nvidia.com/cuda/pdf/EULA.pdf -O $NVIDIA_PATH/NVIDIA_EULA.pdf
	echo 'GPU drivers, CUDA, CuDNN and NLCC installation completed'
	# Install GPU collection in Stackdriver
    install_gpu_agent="$(/usr/share/google/get_metadata_value attributes/install_gpu_agent || false)"
    if [[ "${install_gpu_agent}" == 'true' ]]; then
     install_gpu_agent_service || err "GPU metrics install process failed"
     echo 'GPU agent successfully deployed.'
    else
     echo 'GPU metrics will not be installed.'
     return 0
    fi
  else
    echo 'GPU drivers will be installed only on worker node - skipped for Master node'
    return 0
  fi
}

main