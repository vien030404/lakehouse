<#
.SYNOPSIS
    Đổi retry-policy của cụm Trino (fault-tolerant execution) và tạo lại 3 node.

.DESCRIPTION
    Không sửa file .env: script đặt biến môi trường của shell, thứ Docker
    Compose ưu tiên hơn .env, nên thay đổi chỉ áp dụng cho lần tạo container
    này.

    Container phải được tạo lại (--force-recreate) thay vì restart, vì
    `docker compose restart` giữ nguyên biến môi trường lúc container được tạo.

    retry_policy cũng là session property. Để thử trên vài câu lệnh, có thể
    đặt trực tiếp trong phiên CLI:
        SET SESSION retry_policy = 'TASK';
    Script này dùng khi muốn mọi client (dbt, Superset, DBeaver) chạy dưới
    cùng một chính sách.

.PARAMETER Policy
    NONE  - tắt FTE, chi phí thấp nhất
    TASK  - chạy lại từng task hỏng, cần exchange manager; hợp với batch lớn
    QUERY - chạy lại cả query; hợp với nhiều query ngắn

.EXAMPLE
    .\scripts\set-retry-policy.ps1 TASK
    .\scripts\set-retry-policy.ps1 NONE
    .\scripts\set-retry-policy.ps1          # chỉ xem đang ở chế độ nào
#>

param(
    [Parameter(Position = 0)]
    [ValidateSet('NONE', 'TASK', 'QUERY')]
    [string]$Policy
)

$ErrorActionPreference = 'Stop'
$projectRoot = Split-Path $PSScriptRoot -Parent
Push-Location $projectRoot

try {
    # --- Không truyền tham số: chỉ báo trạng thái hiện tại -----------------
    if (-not $Policy) {
        Write-Host "Chinh sach dang ap dung tren cum:" -ForegroundColor Cyan
        # Đọc biến môi trường của container thay vì parse log: Trino ghi log ra
        # stderr, và PowerShell 5.1 bọc stderr của lệnh native thành
        # ErrorRecord (NativeCommandError).
        $current = docker exec trinodemo-trino-coordinator printenv TRINO_RETRY_POLICY
        if ($LASTEXITCODE -eq 0 -and $current) {
            Write-Host "  $current"
        } else {
            Write-Host "  (khong doc duoc - cum co dang chay khong?)" -ForegroundColor Yellow
        }
        Write-Host ""
        Write-Host "Doi bang:  .\scripts\set-retry-policy.ps1 TASK" -ForegroundColor DarkGray
        exit 0
    }

    Write-Host "Dat retry-policy = $Policy" -ForegroundColor Cyan

    if ($Policy -ne 'NONE') {
        Write-Host ""
        Write-Host "LUU Y: FTE bat Trino ghi/doc lai MinIO giua moi stage." -ForegroundColor Yellow
        Write-Host "Tren may 4 core, moi truy van tuong tac se cham han di." -ForegroundColor Yellow
        Write-Host "Nho tra ve NONE sau khi demo xong." -ForegroundColor Yellow
        Write-Host ""
    }

    # Biến shell được Docker Compose ưu tiên hơn .env
    $env:TRINO_RETRY_POLICY = $Policy

    Write-Host "Dang tao lai 3 node Trino..." -ForegroundColor Cyan
    docker compose --profile core up -d --force-recreate `
        trino-coordinator trino-worker-1 trino-worker-2
    if ($LASTEXITCODE -ne 0) { throw "docker compose that bai" }

    # Coordinator cần khoảng 2 phút để khởi tạo và nạp các catalog
    Write-Host "Cho coordinator san sang (toi da 5 phut)..." -ForegroundColor Cyan
    $deadline = (Get-Date).AddMinutes(5)
    do {
        Start-Sleep -Seconds 10
        $state = docker inspect trinodemo-trino-coordinator `
                    --format '{{.State.Health.Status}}' 2>$null
        Write-Host "  $state"
    } while ($state -ne 'healthy' -and (Get-Date) -lt $deadline)

    if ($state -ne 'healthy') {
        Write-Host "Coordinator chua healthy. Xem log:" -ForegroundColor Red
        Write-Host "  docker logs trinodemo-trino-coordinator" -ForegroundColor Red
        exit 1
    }

    # Xác nhận bằng giá trị đọc lại từ container, không dựa vào biến vừa đặt
    $applied = docker exec trinodemo-trino-coordinator printenv TRINO_RETRY_POLICY

    Write-Host ""
    if ($applied -eq $Policy) {
        Write-Host "XONG. Cum dang chay retry-policy = $applied" -ForegroundColor Green
    } else {
        Write-Host "CANH BAO: yeu cau $Policy nhung cum bao $applied" -ForegroundColor Red
    }

    if ($Policy -ne 'NONE') {
        Write-Host ""
        Write-Host "Thu nghiem: chay mot truy van nang roi giet worker giua chung" -ForegroundColor DarkGray
        Write-Host "  docker stop trinodemo-trino-worker-2" -ForegroundColor DarkGray
        Write-Host "  docker start trinodemo-trino-worker-2" -ForegroundColor DarkGray
        Write-Host "Xem du lieu spooling tren MinIO: bucket 'exchange'" -ForegroundColor DarkGray
    }
}
finally {
    Pop-Location
}
