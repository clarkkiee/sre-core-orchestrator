#!/bin/bash
set -euo pipefail

KEY_DIR="docker/ssh"
KEY_PATH="${KEY_DIR}/id_ed25519"
PUB_PATH="${KEY_PATH}.pub"

echo "==> Setting up orchestrator SSH access"

# 1. Generate keypair 
mkdir -p "${KEY_DIR}"
if [ -f "${KEY_PATH}" ]; then
  echo "    keypair already exists at ${KEY_PATH}, reusing it"
else
  echo "    generating ed25519 keypair at ${KEY_PATH}"
  ssh-keygen -t ed25519 -N "" -C "orchestrator@$(hostname)" -f "${KEY_PATH}"
fi

# 2. Permissions (ssh/asyncssh reject world-readable private keys)
chmod 600 "${KEY_PATH}"
chmod 644 "${PUB_PATH}"

# 3. Authorize the pubkey on the host
AUTH_KEYS="${HOME}/.ssh/authorized_keys"
mkdir -p "${HOME}/.ssh"
chmod 700 "${HOME}/.ssh"
touch "${AUTH_KEYS}"
chmod 600 "${AUTH_KEYS}"

PUB_KEY="$(cat "${PUB_PATH}")"
if grep -qF "${PUB_KEY}" "${AUTH_KEYS}"; then
  echo "    pubkey already present in ${AUTH_KEYS}"
else
  echo "${PUB_KEY}" >> "${AUTH_KEYS}"
  echo "    appended pubkey to ${AUTH_KEYS}"
fi

# 4. Detect the values to put in .env
SSH_USER="$(whoami)"
GW="$(ip -4 route show dev docker0 2>/dev/null | awk '{print $NF}' | head -n1)"
GW="${GW:-172.17.0.1}"

# 5. Warn if sshd is not listening
if ! ss -tlnp 2>/dev/null | grep -q ':22 '; then
  echo ""
  echo "WARNING: no sshd listening on :22 on this host."
  echo "         Install/start it:  sudo apt install -y openssh-server && sudo systemctl enable --now ssh"
  echo "         If a firewall is active, allow the docker bridge:"
  echo "             sudo ufw allow from 172.17.0.0/16 to any port 22"
fi

# 6. Self-test
echo ""
echo "==> Testing SSH from host into ${SSH_USER}@${GW} ..."
if ssh -i "${KEY_PATH}" -o StrictHostKeyChecking=no -o ConnectTimeout=5 \
       "${SSH_USER}@${GW}" true 2>/dev/null; then
  echo "    OK: key-based SSH works"
else
  echo "    FAILED: could not SSH to ${SSH_USER}@${GW} with the generated key."
  echo "    Check that sshd is running and reachable on the docker bridge gateway."
fi

# 7. Print the .env block to use
cat <<EOF

==> Put these values in your .env (gitignored):

    SSH_HOST=${GW}
    AGENT_HOST=${GW}
    SSH_PORT=22
    SSH_USER=${SSH_USER}
    SSH_KEY_PATH=/app/.ssh/id_ed25519   # in-container path; do NOT change

Then recreate the worker so the mount + env reload:

    docker compose -f docker-compose.yml -f docker-compose.dev.yml up -d --force-recreate celery_worker
EOF
