# TP — Kubernetes avec minikube

Exécution complète sur Windows le 15 octobre 2026. Le dossier `preuves` contient les résultats : `RESULTATS-OBSERVES.txt` reprend les sorties observées dans la session, et `rolling-requests.csv` contient l'enregistrement direct des requêtes pendant les mises à jour. Les transcriptions PowerShell seules n'ont pas capturé les sorties des programmes natifs dans cet environnement ; le script a été adapté pour les enregistrer explicitement lors d'une prochaine exécution.

## 1. Installation, démarrage et observation du cluster

Docker, minikube et kubectl étaient déjà installés : minikube **v1.39.0**, kubectl **v1.36.1**, Docker Engine **29.8.0**. Le cluster créé exécute Kubernetes **v1.37.0** avec containerd **2.3.4**.

Le profil préexistant `minikube` utilisait Hyper-V et rencontrait une erreur d'autorisation. Un profil Docker distinct a donc été créé :

```powershell
minikube start -p tp-kubernetes --driver=docker --cpus=2 --memory=2200mb
kubectl config use-context tp-kubernetes
minikube -p tp-kubernetes status
kubectl get nodes
kubectl describe node tp-kubernetes
minikube -p tp-kubernetes addons enable metrics-server
kubectl top pods -A --sort-by=cpu --sum=true
kubectl -n kube-system get pods
```

Le nœud est passé de `NotReady` à **Ready** après l'initialisation du réseau. Son IP interne est **192.168.49.2**. L'API de métriques a nécessité un délai avant de répondre.

**Que fait `kubectl top pods -A --sort-by cpu --sum=true` ?** Elle affiche les consommations CPU et mémoire récentes des pods de tous les namespaces (`-A`), les trie par CPU décroissant et ajoute une ligne de total (`--sum=true`). Ce sont des mesures, pas les limites de ressources configurées. `1000m` représente un cœur CPU ; `Mi` représente des mébioctets.

Mesure obtenue : **152m de CPU et 414Mi de mémoire** au total ; l'application utilisait **4m et 9Mi**. Ces valeurs varient dans le temps.

| Composant observé dans `kube-system` | Rôle |
|---|---|
| kube-apiserver | Point d'entrée de l'API Kubernetes utilisée par kubectl et les composants. |
| etcd | Stockage persistant de l'état et de la configuration du cluster. |
| kube-scheduler | Choix du nœud pour les pods à placer. |
| kube-controller-manager | Contrôleurs qui rapprochent l'état réel de l'état souhaité. |
| kube-proxy | Règles réseau permettant l'accès aux Services. |
| coredns | Résolution des noms DNS dans le cluster. |
| kindnet | Réseau des pods, via le plugin CNI. |
| metrics-server | Collecte des métriques utilisées notamment par `kubectl top`. |
| storage-provisioner | Provisionnement de volumes locaux dans minikube. |

Le **kubelet** s'exécute sur le nœud et pilote les conteneurs via le runtime ; il n'apparaît pas ici comme un pod distinct.

Références : [installation de minikube](https://minikube.sigs.k8s.io/docs/start/) et [composants Kubernetes](https://kubernetes.io/docs/concepts/overview/components/).

## 2. Manipulations avec kubectl

```powershell
kubectl create deployment kubernetes-bootcamp --image=gcr.io/google-samples/kubernetes-bootcamp:v1
kubectl rollout status deployment/kubernetes-bootcamp
kubectl get pods
$POD_NAME = kubectl get pods -l app=kubernetes-bootcamp -o jsonpath='{.items[0].metadata.name}'
kubectl logs $POD_NAME
kubectl describe pod $POD_NAME
kubectl exec $POD_NAME -- cat /etc/os-release
kubectl exec -it $POD_NAME -- bash
```

Dans le shell du conteneur :

```bash
pwd
ls -la
cat /server.js
curl -s http://localhost:8080
exit
```

Le premier pod, **kubernetes-bootcamp-5cc66bcc9b-qk5gt**, était `1/1 Running`. Le répertoire courant est `/` et le fichier source est **`/server.js`**. La ligne `www.listen(8080, ...)` montre que l'application écoute sur **8080**. Un shell interactif a également été ouvert et quitté pendant le TP.

Réponse obtenue dans le premier conteneur :

```text
Hello Kubernetes bootcamp! | Running on: kubernetes-bootcamp-5cc66bcc9b-qk5gt | v=1
```

**Peut-on l'interroger directement depuis Windows ?** Le test de `http://10.244.0.3:8080`, adresse privée du premier pod, a expiré après quatre secondes (code curl 28). Cette adresse n'était pas accessible directement depuis Windows. De même, `localhost` sur Windows ne désigne pas le conteneur. Il faut fournir un accès réseau adapté, ici un Service et un tunnel minikube.

## 3. Exposition du Service

```powershell
kubectl expose deployment/kubernetes-bootcamp --type=NodePort --port=8080
kubectl get services
minikube -p tp-kubernetes ip
minikube -p tp-kubernetes service kubernetes-bootcamp --url
```

Le premier Service avait les valeurs suivantes :

| Propriété | Valeur observée |
|---|---|
| Nom | kubernetes-bootcamp |
| Type | NodePort |
| ClusterIP | 10.104.10.120 |
| Port du Service et port cible | 8080 |
| NodePort attribué | 31867 |
| IP du nœud | 192.168.49.2 |
| URL du premier tunnel | http://127.0.0.1:57658 |

L'adresse théorique du nœud est `http://192.168.49.2:31867`. Avec Docker Desktop sous Windows, le tunnel local a été utilisé, conformément à l'énoncé. Il doit rester actif pendant les requêtes.

```powershell
$env:APP_URL = 'http://127.0.0.1:57658'
curl.exe $env:APP_URL
```

L'accès a fonctionné avec curl **et dans le navigateur**, qui affichait la réponse `v=1` et le nom du pod. Les adresses et ports attribués peuvent changer à la prochaine exécution.

## 4. Augmentation et réduction du nombre de pods

```powershell
kubectl scale deployment/kubernetes-bootcamp --replicas=5
kubectl rollout status deployment/kubernetes-bootcamp
kubectl get pods -l app=kubernetes-bootcamp -o wide
1..10 | ForEach-Object { curl.exe -s -H 'Connection: close' $env:APP_URL }
kubectl scale deployment/kubernetes-bootcamp --replicas=2
kubectl get pods -l app=kubernetes-bootcamp -o wide
```

**Quelle commande vérifie les cinq pods ?** `kubectl get pods -l app=kubernetes-bootcamp` : cinq lignes `1/1 Running` ont été observées après stabilisation.

**Pourquoi les noms changent-ils dans les réponses ?** Le Service dirige les nouvelles connexions vers les pods disponibles correspondant à son sélecteur. Dix requêtes n'impliquent pas une répartition exactement uniforme ni la visite de chaque pod. Lors du premier essai, trois noms différents ont répondu, avec des suffixes `qk5gt`, `sq67l` et `jvggn`. Un navigateur peut réutiliser sa connexion et continuer à atteindre le même pod.

Le retour à deux réplicas déclenche la suppression des trois pods supplémentaires. Ils peuvent rester temporairement visibles en `Terminating` pendant leur délai de fermeture. La vérification du nombre exact a été rejouée séparément après les mises à jour pour éviter de compter leurs pods transitoires.

Résultat final de cette vérification : exactement **deux pods `1/1 Running`**, suffixes `zk6v7` et `zpdhz`, et aucun autre pod de l'application.

## 5. Rolling update et retours arrière

Une boucle de requêtes exécutée dans un processus PowerShell séparé a enregistré les réponses pendant les changements, avec une pause de 500 ms entre deux requêtes. Elle a ensuite été arrêtée. Les résultats sont dans `preuves/rolling-requests.csv`.

```powershell
kubectl set image deployment/kubernetes-bootcamp kubernetes-bootcamp=jocatalin/kubernetes-bootcamp:v2
kubectl rollout status deployment/kubernetes-bootcamp
kubectl set image deployment/kubernetes-bootcamp kubernetes-bootcamp=jocatalin/kubernetes-bootcamp:v3
kubectl get pods
kubectl describe pods -l app=kubernetes-bootcamp
kubectl rollout undo deployment/kubernetes-bootcamp
kubectl rollout status deployment/kubernetes-bootcamp
kubectl rollout history deployment/kubernetes-bootcamp
kubectl rollout history deployment/kubernetes-bootcamp --revision=1
kubectl rollout undo deployment/kubernetes-bootcamp --to-revision=1
kubectl rollout status deployment/kubernetes-bootcamp
```

**Passage à v2.** De nouveaux pods ont remplacé progressivement les anciens. Les réponses sont passées de `v=1` à `v=2`. La boucle a enregistré **23 requêtes, zéro échec**, avec 11 réponses v1 et 12 réponses v2. Aucune interruption n'a été observée par cet échantillonnage ; cela ne prouve pas l'absence de toute coupure plus courte que l'intervalle entre les requêtes.

**Essai de v3.** Le pod `kubernetes-bootcamp-795cf895df-wj926` est resté non prêt avec **ErrImagePull** : le tag demandé ne peut pas être téléchargé. Des tentatives répétées peuvent conduire à `ImagePullBackOff`. Les deux pods v2 restaient prêts et continuaient à servir les requêtes.

Avec deux réplicas, la stratégie RollingUpdate par défaut autorise un pod supplémentaire (`maxSurge=25%`, arrondi supérieur) et aucune indisponibilité planifiée (`maxUnavailable=25%`, arrondi inférieur). Le nouveau pod n'étant pas prêt, les deux anciens sont conservés. Le Service reste disponible, mais la mise à jour est bloquée.

**Premier retour arrière.** `rollout undo` a rétabli l'image v2 ; le déploiement a convergé et les réponses étaient v2.

**Retour à l'image initiale.** La révision 1 identifiait `gcr.io/google-samples/kubernetes-bootcamp:v1`. `--to-revision=1` l'a rétablie. L'historique final affichait les révisions 3, 4 et 5 : restaurer un ancien modèle crée une nouvelle révision. Une réponse v2 a encore été observée immédiatement après le retour, pendant la convergence du routage ; les nouvelles réponses v1 et l'image configurée confirment le retour effectif.

Référence : [Deployments et rolling updates](https://kubernetes.io/docs/concepts/workloads/controllers/deployment/).

## 6. Déploiement déclaratif avec YAML

Les modèles `lab-1` cités dans l'énoncé n'étaient pas joints. Des manifests complets équivalents ont été créés dans `lab-1/deployment.yaml` et `lab-1/service.yaml`.

Le Deployment utilise l'image v1, le label `app: kubernetes-bootcamp`, un sélecteur correspondant et le port conteneur 8080. La version finale fixe **`spec.replicas: 3`**. Le Service de type NodePort sélectionne ces mêmes pods et relie **`port: 8080`** à **`targetPort: 8080`**.

La première application utilise une copie à un réplica conservée dans `preuves/deployment-1-replica.yaml`, puis le manifest final est appliqué pour passer à trois :

```powershell
kubectl delete service kubernetes-bootcamp
kubectl delete deployment kubernetes-bootcamp
kubectl apply -f preuves/deployment-1-replica.yaml
kubectl rollout status deployment/kubernetes-bootcamp
kubectl apply -f lab-1/service.yaml
minikube -p tp-kubernetes service kubernetes-bootcamp --url
kubectl apply -f lab-1/deployment.yaml
kubectl rollout status deployment/kubernetes-bootcamp
kubectl get pods -l app=kubernetes-bootcamp
```

Référence : [Services Kubernetes](https://kubernetes.io/docs/concepts/services-networking/service/).

**Les pods sont-ils actifs après application ?** Oui : un pod était prêt après la première application, puis le Deployment est passé à **3/3 disponibles**. La vérification finale n'affichait plus de pods en terminaison.

**L'accès fonctionne-t-il ?** Oui, avec curl et dans le navigateur, via le nouveau tunnel `http://127.0.0.1:58048`. Le Service recréé a reçu le NodePort **31505**.

**Les requêtes atteignent-elles plusieurs réplicas ?** Oui : vingt requêtes de vérification ont toutes réussi et ont atteint les trois pods `kubernetes-bootcamp-576b7cbb77-cjr99`, `kubernetes-bootcamp-576b7cbb77-cp2rb` et `kubernetes-bootcamp-576b7cbb77-r2xlr`. Les résultats sont repris dans `preuves/RESULTATS-OBSERVES.txt`.

## 7. Nettoyage

Commandes de nettoyage, après arrêt du tunnel :

```powershell
kubectl delete -f lab-1/service.yaml
kubectl delete -f lab-1/deployment.yaml
kubectl get deployment,service,pods
minikube -p tp-kubernetes stop
minikube -p tp-kubernetes status
```

Le Service et le Deployment du TP ont été supprimés, ainsi que leurs pods. La vérification finale ne montrait que le Service système `kubernetes` dans le namespace `default`. Le profil **tp-kubernetes est arrêté** : host, kubelet, apiserver et kubeconfig affichent `Stopped`. Les anciennes URL locales ne sont donc plus actives. Le profil est conservé pour pouvoir redémarrer le TP.

## Fichiers fournis et reproduction

- `lab-1/deployment.yaml` et `lab-1/service.yaml` : manifests complets.
- `run-lab.ps1` : étapes automatisées pour PowerShell 7, avec contexte `tp-kubernetes` explicite.
- `preuves/RESULTATS-OBSERVES.txt` : extraits des sorties réellement observées, regroupés par étape.
- Les autres `preuves/*.txt` : traces PowerShell partielles et informations des tunnels.
- `preuves/rolling-requests.csv` : horodatage, code de sortie et réponse des requêtes pendant les mises à jour.
- `enonce.txt` : copie du sujet fourni.

Pour rejouer le TP, démarrer Docker Desktop et le profil avec la commande de la section 1, puis lancer `./run-lab.ps1 -Stage inspect`, `expose`, `scale`, `rollout`, `manifests`, et enfin `cleanup`, **en attendant la fin de chaque étape**. Depuis un terminal PowerShell 7, remplacer la valeur de `-Stage` à chaque appel. Le script écrit les nouvelles preuves dans le même dossier et ajoute les transcriptions à celles déjà présentes.
