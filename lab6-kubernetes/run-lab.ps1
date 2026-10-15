param([Parameter(Mandatory=$true)][ValidateSet('inspect','expose','scale','rollout','manifests','verify','cleanup')][string]$Stage)
$ErrorActionPreference = 'Stop'
$ProfileName = 'tp-kubernetes'
$Evidence = Join-Path $PSScriptRoot 'preuves'
New-Item -ItemType Directory -Path $Evidence -Force | Out-Null
Start-Transcript -Path (Join-Path $Evidence ($Stage + '.txt')) -Append
function K {
    Add-Content (Join-Path $Evidence ($Stage + '-commands.txt')) ("`n> kubectl --context $ProfileName " + ($args -join ' '))
    & kubectl --context $ProfileName @args | Tee-Object -FilePath (Join-Path $Evidence ($Stage + '-commands.txt')) -Append
    if ($LASTEXITCODE -ne 0) { throw "kubectl a echoue : $args" }
}
function Ready { K rollout status deployment/kubernetes-bootcamp --timeout=300s }
function Requests([int]$Count=10) {
    $url = (Get-Content (Join-Path $Evidence 'app-url.txt') -Raw).Trim()
    1..$Count | ForEach-Object {
        & curl.exe --noproxy '*' --max-time 5 --silent --show-error --fail -H 'Connection: close' $url | Tee-Object -FilePath (Join-Path $Evidence ($Stage + '-http.txt')) -Append
        if ($LASTEXITCODE -ne 0) { throw 'Echec HTTP' }
    }
}
function Tunnel {
    $out = Join-Path $Evidence 'tunnel.stdout.txt'
    $err = Join-Path $Evidence 'tunnel.stderr.txt'
    $proc = Start-Process -FilePath (Get-Command minikube).Source -ArgumentList '-p','tp-kubernetes','service','kubernetes-bootcamp','--url' -WindowStyle Hidden -PassThru -RedirectStandardOutput $out -RedirectStandardError $err
    $proc.Id | Set-Content (Join-Path $Evidence 'tunnel.pid')
    for($n=0;$n -lt 45;$n++) {
        Start-Sleep -Seconds 1
        $data = Get-Content $out -Raw -ErrorAction SilentlyContinue
        if($data -match 'http://127\.0\.0\.1:\d+') {
            $Matches[0] | Set-Content (Join-Path $Evidence 'app-url.txt')
            Write-Output "APP_URL=$($Matches[0])"
            return
        }
        if($proc.HasExited) { Get-Content $err; throw 'Le tunnel a echoue' }
    }
    throw 'URL du tunnel non disponible'
}
function StopTunnel {
    $pidFile = Join-Path $Evidence 'tunnel.pid'
    if(Test-Path $pidFile) {
        $tunnelProcess = Get-Process -Id ([int](Get-Content $pidFile)) -ErrorAction SilentlyContinue
        if($tunnelProcess -and $tunnelProcess.ProcessName -eq 'minikube') { Stop-Process -Id $tunnelProcess.Id }
    }
}
try {
switch($Stage) {
'inspect' {
    minikube -p $ProfileName status
    K get nodes -o wide
    K describe node $ProfileName
    minikube -p $ProfileName addons enable metrics-server
    if($LASTEXITCODE -ne 0) { throw 'metrics-server' }
    K -n kube-system get pods -o wide
    $existing = K get deployment kubernetes-bootcamp --ignore-not-found -o name
    if(-not $existing) { K create deployment kubernetes-bootcamp --image=gcr.io/google-samples/kubernetes-bootcamp:v1 }
    Ready
    K get pods -o wide
    $PodName = K get pods -l app=kubernetes-bootcamp -o 'jsonpath={.items[0].metadata.name}'
    Write-Output "POD_NAME=$PodName"
    K logs $PodName
    K describe pod $PodName
    K exec $PodName '--' cat /etc/os-release
    K exec $PodName '--' bash -c 'pwd; ls -la; cat /server.js; curl -sS http://localhost:8080; exit'
    $PodIp = K get pod $PodName -o 'jsonpath={.status.podIP}'
    Write-Output "Essai depuis Windows vers IP du pod : $PodIp"
    & curl.exe --noproxy '*' --max-time 4 --silent --show-error "http://${PodIp}:8080"
    Write-Output "Code sortie test direct (echec attendu si reseau prive) : $LASTEXITCODE"
    K -n kube-system rollout status deployment/metrics-server --timeout=180s
    for($n=0;$n -lt 12;$n++) {
        & kubectl --context $ProfileName top pods -A --sort-by=cpu --sum=true
        if($LASTEXITCODE -eq 0) { break }
        Start-Sleep -Seconds 10
    }
    if($LASTEXITCODE -ne 0) { throw 'Metriques indisponibles' }
}
'expose' {
    K expose deployment/kubernetes-bootcamp --type=NodePort --port=8080
    K get services -o wide
    minikube -p $ProfileName ip
    Tunnel
    Requests 1
}
'scale' {
    K scale deployment/kubernetes-bootcamp --replicas=5
    Ready
    K get pods -l app=kubernetes-bootcamp -o wide
    Requests 10
    K scale deployment/kubernetes-bootcamp --replicas=2
    Ready
    for($n=0;$n -lt 30;$n++) {
        $items = (K get pods -l app=kubernetes-bootcamp -o json | ConvertFrom-Json).items
        if($items.Count -eq 2) { break }
        Start-Sleep -Seconds 2
    }
    K get pods -l app=kubernetes-bootcamp -o wide
    if($items.Count -ne 2) { throw 'Nombre de pods inattendu' }
}
'rollout' {
    $url = (Get-Content (Join-Path $Evidence 'app-url.txt') -Raw).Trim()
    $monitor = Start-Job -ArgumentList $url -ScriptBlock {
        param($url)
        while($true) {
            $reply = & curl.exe --noproxy '*' --max-time 3 --silent --show-error --fail -H 'Connection: close' $url 2>&1
            [pscustomobject]@{Time=(Get-Date -Format o);ExitCode=$LASTEXITCODE;Response=($reply -join ' ')}
            Start-Sleep -Milliseconds 500
        }
    }
    try {
        Start-Sleep -Seconds 3
        K rollout history deployment/kubernetes-bootcamp
        K set image deployment/kubernetes-bootcamp kubernetes-bootcamp=jocatalin/kubernetes-bootcamp:v2
        Ready
        Requests 3
        K set image deployment/kubernetes-bootcamp kubernetes-bootcamp=jocatalin/kubernetes-bootcamp:v3
        for($n=0;$n -lt 60;$n++) {
            $state = K get pods -l app=kubernetes-bootcamp -o json
            if(($state -join '') -match 'ErrImagePull|ImagePullBackOff') { break }
            Start-Sleep -Seconds 2
        }
        K get pods -l app=kubernetes-bootcamp -o wide
        K describe pods -l app=kubernetes-bootcamp
        Requests 3
        K rollout undo deployment/kubernetes-bootcamp
        Ready
        K get deployment kubernetes-bootcamp -o 'jsonpath={.spec.template.spec.containers[0].image}'
        Requests 2
        K rollout history deployment/kubernetes-bootcamp
        K rollout history deployment/kubernetes-bootcamp --revision=1
        K rollout undo --help
        K rollout undo deployment/kubernetes-bootcamp --to-revision=1
        Ready
        K get deployment kubernetes-bootcamp -o 'jsonpath={.spec.template.spec.containers[0].image}'
        Requests 3
        K rollout history deployment/kubernetes-bootcamp
    } finally {
        Stop-Job $monitor
        $samples = @(Receive-Job $monitor)
        $samples | Select-Object Time,ExitCode,Response | Export-Csv (Join-Path $Evidence 'rolling-requests.csv') -NoTypeInformation -Encoding UTF8
        Write-Output "Requetes pendant mises a jour : $($samples.Count) ; echecs : $(@($samples | Where-Object ExitCode -ne 0).Count)"
        Remove-Job $monitor
    }
}
'manifests' {
    StopTunnel
    K delete service kubernetes-bootcamp
    K delete deployment kubernetes-bootcamp --cascade=foreground --wait=true
    $manifest = Get-Content (Join-Path $PSScriptRoot 'lab-1/deployment.yaml') -Raw
    $single = Join-Path $Evidence 'deployment-1-replica.yaml'
    $manifest.Replace('replicas: 3','replicas: 1') | Set-Content $single -Encoding utf8
    K apply -f $single
    Ready
    K get pods -l app=kubernetes-bootcamp -o wide
    K apply -f (Join-Path $PSScriptRoot 'lab-1/service.yaml')
    Tunnel
    Requests 1
    K apply -f (Join-Path $PSScriptRoot 'lab-1/deployment.yaml')
    Ready
    K get pods -l app=kubernetes-bootcamp -o wide
    Requests 10
    K get 'deployment,service,replicaset,pods' -o wide
    K get deployment kubernetes-bootcamp -o yaml
    K get service kubernetes-bootcamp -o yaml
}
'verify' {
    Ready
    K get 'deployment,service,replicaset,pods' -o wide
    K get deployment kubernetes-bootcamp -o yaml
    K get service kubernetes-bootcamp -o yaml
    $items = (K get pods -l app=kubernetes-bootcamp -o json | ConvertFrom-Json).items
    if($items.Count -ne 3) { throw 'Trois pods attendus' }
    Requests 20
}
'cleanup' {
    StopTunnel
    K delete -f (Join-Path $PSScriptRoot 'lab-1/service.yaml')
    K delete -f (Join-Path $PSScriptRoot 'lab-1/deployment.yaml') --cascade=foreground --wait=true
    K get 'deployment,service,pods'
    minikube -p $ProfileName stop
    if($LASTEXITCODE -ne 0) { throw 'Arret minikube echoue' }
    minikube -p $ProfileName status
}
}
} finally { Stop-Transcript }
