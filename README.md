# UPS shutdown orchestrator for k3s

This project checks the UPS exposed by the NAS through NUT (`upsc`) and, when the configured threshold is reached while the UPS is on battery, it:

1. Suspends CronJobs in selected namespaces.
2. Scales Deployments and StatefulSets in selected namespaces to 0.
3. Waits until the namespaces are quiet.
4. Drains workers.
5. Powers off workers over SSH.
6. Powers off the master.

## Files

- `main.py`: main script
- `config.example.json`: example config
- `ups-shutdown.service`: systemd service
- `ups-shutdown.timer`: systemd timer

## Suggested installation

```bash
sudo mkdir -p /opt/ups-shutdown /etc/ups-shutdown
sudo cp main.py /opt/ups-shutdown/main.py
sudo cp config.example.json /etc/ups-shutdown/config.json
sudo cp ups-shutdown.service /etc/systemd/system/ups-shutdown.service
sudo cp ups-shutdown.timer /etc/systemd/system/ups-shutdown.timer
sudo chmod +x /opt/ups-shutdown/main.py
sudo systemctl daemon-reload
sudo systemctl enable --now ups-shutdown.timer
```

## Test without shutting down anything

```bash
sudo python3 /opt/ups-shutdown/main.py --config /etc/ups-shutdown/config.json --dry-run --force --debug
```

## Notes

- The script assumes it runs on the master and has cluster-admin access through the configured kubeconfig.
- It does **not** touch `longhorn-system` workloads.
- `ignore_owner_kinds=["Cluster"]` is useful for pods managed by CloudNativePG custom resources.
- Workers must accept SSH from the master for the configured user.
