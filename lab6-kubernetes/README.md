# Lab 6: Kubernetes basics with minikube

We deploy a Node.js application on a local Kubernetes cluster, expose it through a NodePort Service, scale it, perform rolling updates and rollbacks, then deploy it using YAML manifests.

## Group

- Group: gr-02
- Lab member 1: Alexis Lainault, git username: XIXIlait
- Lab member 2: Antoine Appourchaux, git username: Antoine924

## Files

| File | Role |
|---|---|
| [COMPTE-RENDU.md](COMPTE-RENDU.md) | Complete report and answers to the lab questions, in French |
| [lab-1/deployment.yaml](lab-1/deployment.yaml) | Deployment using the v1 application image, with 3 replicas |
| [lab-1/service.yaml](lab-1/service.yaml) | NodePort Service targeting application port 8080 |
| [run-lab.ps1](run-lab.ps1) | PowerShell 7 script to reproduce the lab in stages |
| [preuves/RESULTATS-OBSERVES.txt](preuves/RESULTATS-OBSERVES.txt) | Excerpts of the command outputs observed during execution |
| [preuves/rolling-requests.csv](preuves/rolling-requests.csv) | HTTP requests recorded during rolling updates |
| [preuves/deployment-1-replica.yaml](preuves/deployment-1-replica.yaml) | Initial manifest used before scaling to 3 replicas |
| [enonce.txt](enonce.txt) | Lab instructions |

The `lab-1/` subdirectory contains the manifests: its name follows the folder referenced in the original instructions. This submission is **Lab 6**.

## Run

Requirements: Docker Desktop running Linux containers, minikube, kubectl, and PowerShell 7. The recorded execution was performed on Windows on 15 October 2026.

From the repository root, open PowerShell 7 and run:

```powershell
cd lab6-kubernetes
minikube start -p tp-kubernetes --driver=docker --cpus=2 --memory=2200mb
./run-lab.ps1 -Stage inspect
./run-lab.ps1 -Stage expose
./run-lab.ps1 -Stage scale
./run-lab.ps1 -Stage rollout
./run-lab.ps1 -Stage manifests
./run-lab.ps1 -Stage verify
./run-lab.ps1 -Stage cleanup
```

Wait for each stage to finish before starting the next. The script uses the dedicated `tp-kubernetes` context. The `expose` and `manifests` stages start a local tunnel and record its URL in `preuves/app-url.txt`. The `cleanup` stage stops the tunnel, removes the lab workloads and stops minikube.

## Results

- Cluster ready, metrics-server enabled and CPU/memory measurements obtained.
- Application tested inside a pod, from Windows with curl, and in a browser.
- Deployment scaled to 5 replicas and back to 2; requests reached different pods.
- Rolling update to v2 completed. The intentionally missing v3 image caused `ErrImagePull` while v2 remained available.
- Rollbacks to v2 and then the original v1 image completed.
- 23 monitoring requests recorded during the updates, with no failed requests in that sample.
- YAML deployment validated with 3 ready replicas; 20 final requests succeeded and reached all 3 pods.
- Lab workloads removed and minikube stopped after completion.

## Notes

The report and results were imported from the completed Lab 6 submission. The observed-results file contains excerpts, not a complete automatic terminal transcript. The CSV was recorded directly by the monitoring process. The other PowerShell transcripts are partial because native command output was not captured in that environment; the script now explicitly records kubectl and HTTP output for subsequent executions.

Machine-specific metadata was omitted from the submitted transcripts. Local tunnel URLs and process identifiers are temporary and excluded from Git.
