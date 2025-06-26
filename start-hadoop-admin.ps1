# 检查是否以管理员权限运行
if (-NOT ([Security.Principal.WindowsPrincipal] [Security.Principal.WindowsIdentity]::GetCurrent()).IsInRole([Security.Principal.WindowsBuiltInRole] "Administrator"))
{
    # 如果不是管理员，则以管理员权限重新启动脚本
    Write-Host "需要管理员权限，正在重新启动..." -ForegroundColor Yellow
    
    # 构建PowerShell 7的路径
    $pwsh7Path = "pwsh.exe"
    
    # 重新以管理员权限运行当前脚本
    Start-Process -FilePath $pwsh7Path -ArgumentList "-File", $PSCommandPath -Verb RunAs
    exit
}

Write-Host "以管理员权限运行中..." -ForegroundColor Green
Write-Host "PowerShell版本: $($PSVersionTable.PSVersion)" -ForegroundColor Cyan

# 函数：读取INI配置文件
function Read-IniFile {
    param(
        [string]$FilePath
    )
    
    $ini = @{}
    $section = ""
    
    if (Test-Path $FilePath) {
        Get-Content $FilePath | ForEach-Object {
            $line = $_.Trim()
            
            # 跳过注释和空行
            if ($line -match '^#' -or $line -eq '') {
                return
            }
            
            # 检查是否是节标题
            if ($line -match '^\[(.+)\]$') {
                $section = $matches[1]
                $ini[$section] = @{}
            }
            # 检查是否是键值对
            elseif ($line -match '^(.+?)\s*=\s*(.*)$' -and $section -ne "") {
                $key = $matches[1].Trim()
                $value = $matches[2].Trim()
                $ini[$section][$key] = $value
            }
        }
    }
    else {
        Write-Host "⚠️ 配置文件不存在: $FilePath" -ForegroundColor Yellow
    }
    
    return $ini
}

# 函数：设置环境变量
function Set-EnvironmentVariables {
    param(
        [hashtable]$Config
    )
    
    try {
        # 检查配置文件是否存在environment节
        if (-not $Config.ContainsKey("environment")) {
            Write-Host "⚠️ 配置文件中未找到 [environment] 节，使用系统环境变量" -ForegroundColor Yellow
            return $false
        }
        
        $envConfig = $Config["environment"]
        
        # 检查是否启用自定义环境变量配置
        $enableEnvConfig = $true
        if ($envConfig.ContainsKey("enable_env_config")) {
            $enableEnvConfig = $envConfig["enable_env_config"] -eq "true"
        }
        
        if (-not $enableEnvConfig) {
            Write-Host "✓ 环境变量配置已禁用 (enable_env_config = false)" -ForegroundColor Cyan
            Write-Host "使用系统现有环境变量:" -ForegroundColor Cyan
            Write-Host "  JAVA_HOME: $($env:JAVA_HOME)" -ForegroundColor Gray
            Write-Host "  HADOOP_HOME: $($env:HADOOP_HOME)" -ForegroundColor Gray
            return $false
        }
        
        Write-Host "✓ 环境变量配置已启用 (enable_env_config = true)" -ForegroundColor Green
        Write-Host "应用配置文件中的环境变量设置..." -ForegroundColor Yellow
        
        # 设置JAVA_HOME
        if ($envConfig.ContainsKey("java_home") -and $envConfig["java_home"] -ne "") {
            $javaHome = $envConfig["java_home"]
            if (Test-Path $javaHome) {
                $env:JAVA_HOME = $javaHome
                Write-Host "  ✓ JAVA_HOME: $javaHome" -ForegroundColor Green
            }
            else {
                Write-Host "  ⚠️ JAVA_HOME 路径不存在: $javaHome" -ForegroundColor Yellow
            }
        }
        
        # 设置HADOOP_HOME
        if ($envConfig.ContainsKey("hadoop_home") -and $envConfig["hadoop_home"] -ne "") {
            $hadoopHome = $envConfig["hadoop_home"]
            if (Test-Path $hadoopHome) {
                $env:HADOOP_HOME = $hadoopHome
                $env:Path = "$hadoopHome\bin;" + $env:Path
                Write-Host "  ✓ HADOOP_HOME: $hadoopHome" -ForegroundColor Green
            }
            else {
                Write-Host "  ⚠️ HADOOP_HOME 路径不存在: $hadoopHome" -ForegroundColor Yellow
            }
        }
        
        # 设置PYSPARK相关环境变量（可选）
        if ($envConfig.ContainsKey("pyspark_python") -and $envConfig["pyspark_python"] -ne "") {
            $pysparkPython = $envConfig["pyspark_python"]
            if (Test-Path $pysparkPython) {
                $env:PYSPARK_PYTHON = $pysparkPython
                Write-Host "  ✓ PYSPARK_PYTHON: $pysparkPython" -ForegroundColor Green
            }
        }
        
        if ($envConfig.ContainsKey("pyspark_driver_python") -and $envConfig["pyspark_driver_python"] -ne "") {
            $pysparkDriverPython = $envConfig["pyspark_driver_python"]
            if (Test-Path $pysparkDriverPython) {
                $env:PYSPARK_DRIVER_PYTHON = $pysparkDriverPython
                Write-Host "  ✓ PYSPARK_DRIVER_PYTHON: $pysparkDriverPython" -ForegroundColor Green
            }
        }
        
        return $true
    }
    catch {
        Write-Host "⚠️ 设置环境变量时出现错误: $($_.Exception.Message)" -ForegroundColor Red
        return $false
    }
}

try {
    # 读取配置文件
    $configPath = ".\config\config.ini"
    Write-Host "正在读取配置文件: $configPath" -ForegroundColor Yellow
    
    $config = Read-IniFile -FilePath $configPath
    
    # 设置环境变量
    $envConfigured = Set-EnvironmentVariables -Config $config
    
    # 如果配置文件中没有设置环境变量，使用默认值
    if (-not $envConfigured) {
        Write-Host "使用默认环境变量设置..." -ForegroundColor Yellow
        
        # 检查是否已经设置了系统环境变量
        if (-not $env:JAVA_HOME) {
            $env:JAVA_HOME = "C:\Environment\JDK\temurin-1.8.0_432"
            Write-Host "  设置默认 JAVA_HOME: $env:JAVA_HOME" -ForegroundColor Gray
        }
        
        if (-not $env:HADOOP_HOME) {
            $env:HADOOP_HOME = "C:\Environment\JDK\hadoop-3.1.0"
            $env:Path = "$env:HADOOP_HOME\bin;" + $env:Path
            Write-Host "  设置默认 HADOOP_HOME: $env:HADOOP_HOME" -ForegroundColor Gray
        }
    }
    
    # 显示最终的环境变量设置
    Write-Host "`n当前环境变量设置:" -ForegroundColor Cyan
    Write-Host "  JAVA_HOME: $env:JAVA_HOME" -ForegroundColor Cyan
    Write-Host "  HADOOP_HOME: $env:HADOOP_HOME" -ForegroundColor Cyan
    
    # 验证关键路径是否存在
    $pathsValid = $true
    if (-not (Test-Path $env:JAVA_HOME)) {
        Write-Host "  ❌ JAVA_HOME 路径不存在!" -ForegroundColor Red
        $pathsValid = $false
    }
    if (-not (Test-Path $env:HADOOP_HOME)) {
        Write-Host "  ❌ HADOOP_HOME 路径不存在!" -ForegroundColor Red
        $pathsValid = $false
    }
    
    if (-not $pathsValid) {
        Write-Host "`n⚠️ 检测到路径问题，请检查配置文件或系统环境变量" -ForegroundColor Yellow
        Write-Host "是否继续启动Hadoop? (y/n): " -ForegroundColor Yellow -NoNewline
        $continue = Read-Host
        if ($continue -ne "y" -and $continue -ne "Y") {
            Write-Host "已取消启动" -ForegroundColor Gray
            exit
        }
    }
    
    # 切换到Hadoop目录
    Write-Host "`n切换到Hadoop目录..." -ForegroundColor Yellow
    Set-Location $env:HADOOP_HOME
    
    # 启动Hadoop DFS
    Write-Host "启动Hadoop DFS..." -ForegroundColor Yellow
    & ".\sbin\start-dfs.cmd"
    
    Write-Host "Hadoop DFS启动完成!" -ForegroundColor Green
}
catch {
    Write-Host "执行过程中出现错误: $($_.Exception.Message)" -ForegroundColor Red
    Write-Host "错误详情: $($_.ScriptStackTrace)" -ForegroundColor Red
}

# 用户选择操作
do {
    Write-Host "`n请选择操作:" -ForegroundColor Yellow
    Write-Host "exit. 退出" -ForegroundColor White
    Write-Host "stop. 停止Hadoop DFS (执行 .\sbin\stop-dfs.cmd)" -ForegroundColor White
    Write-Host "env.  显示当前环境变量" -ForegroundColor White
    Write-Host "请输入选择 (exit/stop/env): " -ForegroundColor Cyan -NoNewline

    $choice = Read-Host
    
    switch ($choice) {
        "exit" {
            Write-Host "正在退出..." -ForegroundColor Gray
            break
        }
        "stop" {
            try {
                Write-Host "正在停止Hadoop DFS..." -ForegroundColor Yellow
                & ".\sbin\stop-dfs.cmd"
                Write-Host "Hadoop DFS已停止!" -ForegroundColor Green
            }
            catch {
                Write-Host "停止Hadoop DFS时出现错误: $($_.Exception.Message)" -ForegroundColor Red
            }
            break
        }
        "env" {
            Write-Host "`n当前环境变量:" -ForegroundColor Cyan
            Write-Host "  JAVA_HOME: $env:JAVA_HOME" -ForegroundColor White
            Write-Host "  HADOOP_HOME: $env:HADOOP_HOME" -ForegroundColor White
            Write-Host "  PYSPARK_PYTHON: $env:PYSPARK_PYTHON" -ForegroundColor White
            Write-Host "  PYSPARK_DRIVER_PYTHON: $env:PYSPARK_DRIVER_PYTHON" -ForegroundColor White
            
            # 显示路径是否存在
            Write-Host "`n路径验证:" -ForegroundColor Cyan
            @{
                "JAVA_HOME" = $env:JAVA_HOME
                "HADOOP_HOME" = $env:HADOOP_HOME
            }.GetEnumerator() | ForEach-Object {
                $status = if (Test-Path $_.Value) { "✓" } else { "❌" }
                Write-Host "  $status $($_.Key): $($_.Value)" -ForegroundColor $(if (Test-Path $_.Value) { "Green" } else { "Red" })
            }
        }
        default {
            Write-Host "无效选择，请输入exit、stop或env" -ForegroundColor Red
        }
    }
} while ($choice -ne "exit" -and $choice -ne "stop")