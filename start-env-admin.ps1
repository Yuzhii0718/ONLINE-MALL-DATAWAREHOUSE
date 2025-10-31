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

# 函数：检查MySQL服务状态
function Test-MySQLService {
    param(
        [string]$ServiceName
    )
    
    try {
        $service = Get-Service -Name $ServiceName -ErrorAction SilentlyContinue
        if ($null -eq $service) {
            return @{
                Exists = $false
                Running = $false
                Status = "NotFound"
            }
        }
        
        return @{
            Exists = $true
            Running = ($service.Status -eq 'Running')
            Status = $service.Status
            Service = $service
        }
    }
    catch {
        return @{
            Exists = $false
            Running = $false
            Status = "Error"
            Error = $_.Exception.Message
        }
    }
}

# 函数：启动MySQL服务
function Start-MySQLService {
    param(
        [string]$ServiceName
    )
    
    try {
        Write-Host "正在启动MySQL服务: $ServiceName ..." -ForegroundColor Yellow
        Start-Service -Name $ServiceName -ErrorAction Stop
        
        # 等待服务启动
        $timeout = 30
        $elapsed = 0
        while ($elapsed -lt $timeout) {
            $service = Get-Service -Name $ServiceName
            if ($service.Status -eq 'Running') {
                Write-Host "✓ MySQL服务启动成功!" -ForegroundColor Green
                return $true
            }
            Start-Sleep -Seconds 1
            $elapsed++
        }
        
        Write-Host "⚠️ MySQL服务启动超时" -ForegroundColor Yellow
        return $false
    }
    catch {
        Write-Host "❌ 启动MySQL服务失败: $($_.Exception.Message)" -ForegroundColor Red
        return $false
    }
}

# 函数：检查MySQL连接
function Test-MySQLConnection {
    param(
        [hashtable]$MySQLConfig
    )
    
    try {
        $mysqlHost = $MySQLConfig["host"]
        $mysqlPort = $MySQLConfig["port"]
        $mysqlUsername = $MySQLConfig["username"]
        $mysqlPassword = $MySQLConfig["password"]
        
        # 使用mysql命令行客户端测试连接（如果可用）
        $mysqlCmd = Get-Command mysql -ErrorAction SilentlyContinue
        if ($mysqlCmd) {
            Write-Host "尝试使用mysql命令行客户端连接..." -ForegroundColor Gray
            $testQuery = "SELECT 1"
            $result = & mysql -h $mysqlHost -P $mysqlPort -u $mysqlUsername -p"$mysqlPassword" -e $testQuery 2>&1
            if ($LASTEXITCODE -eq 0) {
                Write-Host "✓ MySQL连接测试成功" -ForegroundColor Green
                return $true
            }
            else {
                Write-Host "⚠️ MySQL命令行连接失败，尝试端口连接测试..." -ForegroundColor Yellow
            }
        }
        
        # 如果没有mysql命令或连接失败，尝试TCP端口连接测试
        Write-Host "测试MySQL端口连接 (${mysqlHost}:${mysqlPort})..." -ForegroundColor Gray
        $tcpClient = New-Object System.Net.Sockets.TcpClient
        $connect = $tcpClient.BeginConnect($mysqlHost, $mysqlPort, $null, $null)
        $wait = $connect.AsyncWaitHandle.WaitOne(3000, $false)
        
        if ($wait) {
            try {
                $tcpClient.EndConnect($connect)
                $tcpClient.Close()
                Write-Host "✓ MySQL端口连接正常 (${mysqlHost}:${mysqlPort})" -ForegroundColor Green
                return $true
            }
            catch {
                $tcpClient.Close()
                Write-Host "⚠️ MySQL端口连接失败 (${mysqlHost}:${mysqlPort})" -ForegroundColor Yellow
                return $false
            }
        }
        else {
            $tcpClient.Close()
            Write-Host "⚠️ MySQL端口连接超时 (${mysqlHost}:${mysqlPort})" -ForegroundColor Yellow
            return $false
        }
    }
    catch {
        Write-Host "⚠️ MySQL连接测试出错: $($_.Exception.Message)" -ForegroundColor Yellow
        return $false
    }
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
                # 将JAVA_HOME\bin加入PATH，便于使用jps等工具
                $env:Path = "$javaHome\bin;" + $env:Path
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

# 函数：检查Hadoop DFS是否运行（使用JAVA_HOME下的jps.exe）
function Test-HadoopDFS {
    try {
        $jpsPath = "$env:JAVA_HOME\bin\jps.exe"
        if (Test-Path $jpsPath) {
            Write-Host "使用JPS检查Hadoop进程: $jpsPath" -ForegroundColor Cyan
            $processes = & $jpsPath
            $hasNameNode = $processes -match "NameNode"
            $hasDataNode = $processes -match "DataNode"

            if ($hasNameNode -and $hasDataNode) {
                Write-Host "✓ Hadoop DFS 正在运行" -ForegroundColor Green
                Write-Host "  检测到进程:" -ForegroundColor Green
                $processes | Where-Object { $_ -match "NameNode|DataNode" } | ForEach-Object {
                    Write-Host "    $_" -ForegroundColor Green
                }
                return $true
            }
            else {
                Write-Host "⚠️ Hadoop DFS 未完全运行" -ForegroundColor Yellow
                if (-not $hasNameNode) { Write-Host "  - NameNode 未运行" -ForegroundColor Yellow }
                if (-not $hasDataNode) { Write-Host "  - DataNode 未运行" -ForegroundColor Yellow }
                return $false
            }
        }
        else {
            Write-Host "⚠️ 无法找到jps命令: $jpsPath" -ForegroundColor Red
            Write-Host "  请检查JAVA_HOME是否正确设置: $env:JAVA_HOME" -ForegroundColor Yellow
            return $false
        }
    }
    catch {
        Write-Host "⚠️ 检查Hadoop状态时出错: $($_.Exception.Message)" -ForegroundColor Yellow
        return $false
    }
}

# 函数：关闭HDFS安全模式（SafeMode）
function Disable-HDFSSafeMode {
    try {
        # 优先使用 HADOOP_HOME 下的 hadoop.cmd；若不存在则回退到 PATH 中的 hadoop/hdfs
        $hadoopCmdCandidates = @(
            (Join-Path $env:HADOOP_HOME 'bin\hadoop.cmd'),
            (Join-Path $env:HADOOP_HOME 'bin\hdfs.cmd'),
            'hadoop',
            'hdfs'
        )

        $cli = $null
        foreach ($c in $hadoopCmdCandidates) {
            if ($c -match '\\' ) {
                if (Test-Path $c) { $cli = $c; break }
            } else {
                $cmd = Get-Command $c -ErrorAction SilentlyContinue
                if ($cmd) { $cli = $cmd.Source; break }
            }
        }

        if (-not $cli) {
            Write-Host "❌ 未找到 hadoop/hdfs 可执行文件，请检查 HADOOP_HOME 或 PATH" -ForegroundColor Red
            return $false
        }

        Write-Host "使用命令关闭HDFS安全模式: $cli dfsadmin -safemode leave" -ForegroundColor Yellow

        # 关闭前获取一次状态
        try {
            $before = & $cli dfsadmin -safemode get 2>&1
            if ($LASTEXITCODE -eq 0) { Write-Host "当前安全模式状态: $before" -ForegroundColor Gray }
        } catch {}

        # 执行关闭安全模式
        $output = & $cli dfsadmin -safemode leave 2>&1
        Write-Host $output

        # 再次确认状态
        try {
            $after = & $cli dfsadmin -safemode get 2>&1
            if ($after -match 'Safe mode is OFF') {
                Write-Host "✓ 已关闭HDFS安全模式" -ForegroundColor Green
                return $true
            } else {
                Write-Host "⚠️ 关闭后状态: $after" -ForegroundColor Yellow
                Write-Host "如仍为ON，请确认NameNode已完全启动且无写入阻塞" -ForegroundColor Yellow
                return $false
            }
        } catch {
            Write-Host "⚠️ 获取安全模式状态失败，可能已关闭: $($_.Exception.Message)" -ForegroundColor Yellow
            return $true
        }
    }
    catch {
        Write-Host "❌ 关闭HDFS安全模式时出错: $($_.Exception.Message)" -ForegroundColor Red
        return $false
    }
}

# 函数：以“分离进程/新窗口”的方式启动Hadoop DFS，避免与当前会话绑定
function Start-HadoopDFSDetached {
    try {
        if (-not $env:HADOOP_HOME) {
            Write-Host "❌ 未设置 HADOOP_HOME，无法启动Hadoop" -ForegroundColor Red
            return $false
        }
        $hadoopHome = $env:HADOOP_HOME
        $startScript = Join-Path $hadoopHome 'sbin\start-dfs.cmd'
        if (-not (Test-Path $startScript)) {
            Write-Host "❌ 未找到启动脚本: $startScript" -ForegroundColor Red
            return $false
        }

        Write-Host "以分离方式启动 Hadoop DFS..." -ForegroundColor Yellow
        # 方式1：直接以独立进程启动脚本（窗口最小化）
        Start-Process -FilePath $startScript -WorkingDirectory $hadoopHome -WindowStyle Minimized

        # 可选：等待几秒并校验
        Start-Sleep -Seconds 2
        $ok = Test-HadoopDFS
        if ($ok) {
            Write-Host "✓ Hadoop DFS 启动成功（分离进程）" -ForegroundColor Green
            return $true
        } else {
            Write-Host "⚠️ Hadoop DFS 启动校验未通过，请查看新窗口输出或日志" -ForegroundColor Yellow
            return $false
        }
    }
    catch {
        Write-Host "❌ 启动Hadoop DFS失败: $($_.Exception.Message)" -ForegroundColor Red
        return $false
    }
}

# 函数：停止MySQL服务
function Stop-MySQLService {
    param(
        [string]$ServiceName
    )

    try {
        Write-Host "正在停止MySQL服务: $ServiceName ..." -ForegroundColor Yellow
        Stop-Service -Name $ServiceName -ErrorAction Stop

        # 等待服务停止
        $timeout = 30
        $elapsed = 0
        while ($elapsed -lt $timeout) {
            $service = Get-Service -Name $ServiceName -ErrorAction SilentlyContinue
            if ($null -ne $service -and $service.Status -eq 'Stopped') {
                Write-Host "✓ MySQL服务已停止" -ForegroundColor Green
                return $true
            }
            Start-Sleep -Seconds 1
            $elapsed++
        }

        Write-Host "⚠️ 停止MySQL服务超时" -ForegroundColor Yellow
        return $false
    }
    catch {
        Write-Host "❌ 停止MySQL服务失败: $($_.Exception.Message)" -ForegroundColor Red
        return $false
    }
}

# 函数：打开带有已配置环境变量的新命令窗口（默认目录为HADOOP_HOME）
function Open-EnvShell {
    param(
        [ValidateSet('pwsh','cmd')]
        [string]$Shell = 'pwsh'
    )

    try {
        $workDir = if ($env:HADOOP_HOME -and (Test-Path $env:HADOOP_HOME)) { $env:HADOOP_HOME } else { (Get-Location).Path }
        if (-not ($env:HADOOP_HOME)) {
            Write-Host "⚠️ HADOOP_HOME 未设置，使用当前目录作为默认目录: $workDir" -ForegroundColor Yellow
        } elseif (-not (Test-Path $env:HADOOP_HOME)) {
            Write-Host "⚠️ HADOOP_HOME 路径不存在，使用当前目录作为默认目录: $workDir" -ForegroundColor Yellow
        }

        switch ($Shell) {
            'pwsh' {
                $pwshExe = 'pwsh.exe'
                $args = @('-NoExit','-NoLogo')
                Start-Process -FilePath $pwshExe -ArgumentList $args -WorkingDirectory $workDir
                Write-Host "✓ 已打开 PowerShell 窗口（默认目录: $workDir）" -ForegroundColor Green
            }
            'cmd' {
                $cmdExe = 'cmd.exe'
                # /K 保持窗口；设置标题并切换到工作目录
                $cmdLine = "title GMALL-Hadoop-Env && cd /d `"$workDir`""
                Start-Process -FilePath $cmdExe -ArgumentList @('/K', $cmdLine) -WorkingDirectory $workDir
                Write-Host "✓ 已打开 CMD 窗口（默认目录: $workDir）" -ForegroundColor Green
            }
        }
        return $true
    }
    catch {
        Write-Host "❌ 打开新命令窗口失败: $($_.Exception.Message)" -ForegroundColor Red
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

        # 确保JAVA_HOME\bin在PATH中，便于调用jps
        if ($env:JAVA_HOME -and (Test-Path $env:JAVA_HOME)) {
            $env:Path = "$env:JAVA_HOME\bin;" + $env:Path
        }
    }
    
    # ========== 检查和启动MySQL ==========
    Write-Host "`n==================== MySQL检查 ====================" -ForegroundColor Cyan
    
    # 获取MySQL配置
    $mysqlServiceName = "MySQL83"  # 默认值
    if ($config.ContainsKey("mysql") -and $config["mysql"].ContainsKey("service_name")) {
        $mysqlServiceName = $config["mysql"]["service_name"]
    }
    
    Write-Host "检查MySQL服务: $mysqlServiceName" -ForegroundColor Yellow
    
    # 检查MySQL服务状态
    $mysqlStatus = Test-MySQLService -ServiceName $mysqlServiceName
    
    if (-not $mysqlStatus.Exists) {
        Write-Host "⚠️ MySQL服务不存在: $mysqlServiceName" -ForegroundColor Yellow
        Write-Host "请检查配置文件中的service_name设置，或手动启动MySQL" -ForegroundColor Yellow
    }
    elseif ($mysqlStatus.Running) {
        Write-Host "✓ MySQL服务已运行 (状态: $($mysqlStatus.Status))" -ForegroundColor Green
        
        # 测试MySQL连接
        if ($config.ContainsKey("mysql")) {
            Test-MySQLConnection -MySQLConfig $config["mysql"]
        }
    }
    else {
        Write-Host "⚠️ MySQL服务未运行 (状态: $($mysqlStatus.Status))" -ForegroundColor Yellow
        Write-Host "正在尝试启动MySQL服务..." -ForegroundColor Yellow
        
        $started = Start-MySQLService -ServiceName $mysqlServiceName
        
        if ($started -and $config.ContainsKey("mysql")) {
            # 等待2秒让MySQL完全启动
            Start-Sleep -Seconds 2
            Test-MySQLConnection -MySQLConfig $config["mysql"]
        }
    }
    
    Write-Host "==================================================`n" -ForegroundColor Cyan
    
    # 显示最终的环境变量设置
    Write-Host "当前环境变量设置:" -ForegroundColor Cyan
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
    
    # ========== 启动Hadoop ==========
    Write-Host "`n==================== Hadoop启动 ====================" -ForegroundColor Cyan
    
    # ========== 启动Hadoop ==========
    Write-Host "`n==================== Hadoop启动 ====================" -ForegroundColor Cyan
    
    # 切换到Hadoop目录
    Write-Host "切换到Hadoop目录..." -ForegroundColor Yellow
    Set-Location $env:HADOOP_HOME
    
    # 启动Hadoop DFS（使用分离进程，避免随当前会话关闭）
    Write-Host "启动Hadoop DFS..." -ForegroundColor Yellow
    $started = Start-HadoopDFSDetached
    if (-not $started) {
        Write-Host "⚠️ 启动校验失败，但可能仍已启动，请使用 'status' 再次检查" -ForegroundColor Yellow
    }
    Write-Host "✓ Hadoop DFS启动流程结束" -ForegroundColor Green
    Write-Host "==================================================`n" -ForegroundColor Cyan
}
catch {
    Write-Host "执行过程中出现错误: $($_.Exception.Message)" -ForegroundColor Red
    Write-Host "错误详情: $($_.ScriptStackTrace)" -ForegroundColor Red
}

# 用户选择操作
do {
    Write-Host "`n请选择操作:" -ForegroundColor Yellow
    Write-Host "exit.  退出" -ForegroundColor White
    Write-Host "stop.  停止Hadoop DFS" -ForegroundColor White
    Write-Host "mysql. 检查MySQL状态" -ForegroundColor White
    Write-Host "mysql-stop. 停止MySQL服务" -ForegroundColor White
    Write-Host "status. 检查Hadoop状态" -ForegroundColor White
    Write-Host "shell. 打开PowerShell环境窗口" -ForegroundColor White
    Write-Host "cmd.   打开CMD环境窗口" -ForegroundColor White
    Write-Host "safemode-leave. 关闭HDFS安全模式" -ForegroundColor White
    Write-Host "env.   显示当前环境变量" -ForegroundColor White
    Write-Host "请输入选择 (exit/stop/mysql/mysql-stop/status/shell/cmd/safemode-leave/env): " -ForegroundColor Cyan -NoNewline

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
                Write-Host "✓ Hadoop DFS已停止!" -ForegroundColor Green
            }
            catch {
                Write-Host "❌ 停止Hadoop DFS时出现错误: $($_.Exception.Message)" -ForegroundColor Red
            }
            break
        }
        "mysql" {
            Write-Host "`nMySQL状态检查:" -ForegroundColor Cyan
            
            # 获取MySQL配置
            $mysqlServiceName = "MySQL83"
            if ($config.ContainsKey("mysql") -and $config["mysql"].ContainsKey("service_name")) {
                $mysqlServiceName = $config["mysql"]["service_name"]
            }
            
            $mysqlStatus = Test-MySQLService -ServiceName $mysqlServiceName
            
            if (-not $mysqlStatus.Exists) {
                Write-Host "  ❌ MySQL服务不存在: $mysqlServiceName" -ForegroundColor Red
            }
            elseif ($mysqlStatus.Running) {
                Write-Host "  ✓ MySQL服务正在运行 (状态: $($mysqlStatus.Status))" -ForegroundColor Green
                
                # 显示MySQL配置信息
                if ($config.ContainsKey("mysql")) {
                    $mysqlConfig = $config["mysql"]
                    Write-Host "  配置信息:" -ForegroundColor Cyan
                    Write-Host "    主机: $($mysqlConfig['host'])" -ForegroundColor White
                    Write-Host "    端口: $($mysqlConfig['port'])" -ForegroundColor White
                    Write-Host "    数据库: $($mysqlConfig['database'])" -ForegroundColor White
                    Write-Host "    用户名: $($mysqlConfig['username'])" -ForegroundColor White
                    
                    # 测试连接
                    Test-MySQLConnection -MySQLConfig $mysqlConfig
                }
            }
            else {
                Write-Host "  ⚠️ MySQL服务未运行 (状态: $($mysqlStatus.Status))" -ForegroundColor Yellow
                Write-Host "  是否启动MySQL服务? (y/n): " -ForegroundColor Yellow -NoNewline
                $startMySQL = Read-Host
                
                if ($startMySQL -eq "y" -or $startMySQL -eq "Y") {
                    $started = Start-MySQLService -ServiceName $mysqlServiceName
                    if ($started -and $config.ContainsKey("mysql")) {
                        Start-Sleep -Seconds 2
                        Test-MySQLConnection -MySQLConfig $config["mysql"]
                    }
                }
            }
        }
        "mysql-stop" {
            Write-Host "`n停止MySQL服务:" -ForegroundColor Cyan
            $mysqlServiceName = "MySQL83"
            if ($config.ContainsKey("mysql") -and $config["mysql"].ContainsKey("service_name")) {
                $mysqlServiceName = $config["mysql"]["service_name"]
            }
            $stopped = Stop-MySQLService -ServiceName $mysqlServiceName
            if (-not $stopped) {
                Write-Host "  ⚠️ 如果服务未停止，请以管理员身份重试或检查服务名称" -ForegroundColor Yellow
            }
        }
        "status" {
            Write-Host "`n检查Hadoop DFS状态..." -ForegroundColor Yellow
            $hadoopStatus = Test-HadoopDFS
            if ($hadoopStatus) {
                Write-Host "Hadoop DFS 运行正常" -ForegroundColor Green
            }
            else {
                Write-Host "Hadoop DFS 未运行或状态异常" -ForegroundColor Red
                Write-Host "提示：可运行 start-env-admin.ps1 以启动，或检查JAVA_HOME/jps" -ForegroundColor Yellow
            }
        }
        "shell" {
            Write-Host "`n打开 PowerShell 环境窗口..." -ForegroundColor Yellow
            [void](Open-EnvShell -Shell 'pwsh')
        }
        "cmd" {
            Write-Host "`n打开 CMD 环境窗口..." -ForegroundColor Yellow
            [void](Open-EnvShell -Shell 'cmd')
        }
        "safemode-leave" {
            Write-Host "`n关闭HDFS安全模式 (SafeMode)..." -ForegroundColor Yellow
            $ok = Disable-HDFSSafeMode
            if (-not $ok) {
                Write-Host "如果失败，请确认 NameNode 正常、Hadoop 命令可用，并稍后重试" -ForegroundColor Yellow
            }
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
            Write-Host "无效选择，请输入 exit、stop、mysql、mysql-stop、status 或 env" -ForegroundColor Red
        }
    }
} while ($choice -ne "exit" -and $choice -ne "stop")