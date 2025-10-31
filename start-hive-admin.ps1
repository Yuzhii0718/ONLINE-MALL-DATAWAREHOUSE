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

# 函数：设置Hive环境变量
function Set-HiveEnvironment {
    param(
        [hashtable]$Config
    )
    
    try {
        # 检查配置文件是否存在environment节
        if (-not $Config.ContainsKey("environment")) {
            Write-Host "⚠️ 配置文件中未找到 [environment] 节，使用默认路径" -ForegroundColor Yellow
            # 使用默认路径
            $env:HIVE_HOME = "C:\Environment\JDK\hive-3.1.0"
            $env:HADOOP_HOME = "C:\Environment\JDK\hadoop-3.1.0"
            $env:JAVA_HOME = "C:\Environment\JDK\temurin-1.8.0_432"
        }
        else {
            $envConfig = $Config["environment"]
            
            # 设置HIVE_HOME
            if ($envConfig.ContainsKey("hive_home") -and $envConfig["hive_home"] -ne "") {
                $env:HIVE_HOME = $envConfig["hive_home"]
            }
            else {
                $env:HIVE_HOME = "C:\Environment\JDK\hive-3.1.0"
            }
            
            # 设置HADOOP_HOME
            if ($envConfig.ContainsKey("hadoop_home") -and $envConfig["hadoop_home"] -ne "") {
                $env:HADOOP_HOME = $envConfig["hadoop_home"]
            }
            else {
                $env:HADOOP_HOME = "C:\Environment\JDK\hadoop-3.1.0"
            }
            
            # 设置JAVA_HOME
            if ($envConfig.ContainsKey("java_home") -and $envConfig["java_home"] -ne "") {
                $env:JAVA_HOME = $envConfig["java_home"]
            }
            else {
                $env:JAVA_HOME = "C:\Environment\JDK\temurin-1.8.0_432"
            }
        }
        
        # 添加关键目录到PATH (Hive, Hadoop, Java)
        $env:Path = "$env:HIVE_HOME\bin;$env:HADOOP_HOME\bin;$env:JAVA_HOME\bin;" + $env:Path
        
        # 验证路径是否存在
        $pathsValid = $true
        
        if (-not (Test-Path $env:HIVE_HOME)) {
            Write-Host "  ❌ HIVE_HOME 路径不存在: $env:HIVE_HOME" -ForegroundColor Red
            $pathsValid = $false
        }
        else {
            Write-Host "  ✓ HIVE_HOME: $env:HIVE_HOME" -ForegroundColor Green
        }
        
        if (-not (Test-Path $env:HADOOP_HOME)) {
            Write-Host "  ❌ HADOOP_HOME 路径不存在: $env:HADOOP_HOME" -ForegroundColor Red
            $pathsValid = $false
        }
        else {
            Write-Host "  ✓ HADOOP_HOME: $env:HADOOP_HOME" -ForegroundColor Green
        }
        
        if (-not (Test-Path $env:JAVA_HOME)) {
            Write-Host "  ❌ JAVA_HOME 路径不存在: $env:JAVA_HOME" -ForegroundColor Red
            $pathsValid = $false
        }
        else {
            Write-Host "  ✓ JAVA_HOME: $env:JAVA_HOME" -ForegroundColor Green
        }
        
        return $pathsValid
    }
    catch {
        Write-Host "⚠️ 设置环境变量时出现错误: $($_.Exception.Message)" -ForegroundColor Red
        return $false
    }
}

# 函数：检查Hadoop DFS是否运行
function Test-HadoopDFS {
    try {
        # 确保JAVA_HOME\bin在PATH中以便找到jps命令
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
                Write-Host "  提示: 请先运行 start-env-admin.ps1 启动Hadoop" -ForegroundColor Yellow
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

# 函数：启动Hive客户端（新窗口）
function Start-HiveClient {
    try {
        Write-Host "`n准备启动Hive客户端..." -ForegroundColor Yellow
        Write-Host "将在新窗口中打开Hive CLI" -ForegroundColor Cyan
        
        # 创建启动脚本
        $startScript = @"
`$env:HIVE_HOME = "$env:HIVE_HOME"
`$env:HADOOP_HOME = "$env:HADOOP_HOME"
`$env:JAVA_HOME = "$env:JAVA_HOME"
`$env:Path = "`$env:HIVE_HOME\bin;`$env:HADOOP_HOME\bin;`$env:JAVA_HOME\bin;" + `$env:Path

Write-Host "========================================" -ForegroundColor Cyan
Write-Host "       Hive CLI 已启动" -ForegroundColor Green
Write-Host "========================================" -ForegroundColor Cyan
Write-Host "环境变量:" -ForegroundColor Yellow
Write-Host "  HIVE_HOME: `$env:HIVE_HOME" -ForegroundColor White
Write-Host "  HADOOP_HOME: `$env:HADOOP_HOME" -ForegroundColor White
Write-Host "  JAVA_HOME: `$env:JAVA_HOME" -ForegroundColor White
Write-Host "========================================`n" -ForegroundColor Cyan

Set-Location "`$env:HIVE_HOME\bin"
& ".\hive.cmd"
"@
        
        # 保存临时启动脚本
        $tempScript = [System.IO.Path]::GetTempFileName() + ".ps1"
        Set-Content -Path $tempScript -Value $startScript -Encoding UTF8
        
        # 在新窗口中启动Hive
        Start-Process -FilePath "pwsh.exe" -ArgumentList "-NoExit", "-File", $tempScript
        
        Write-Host "✓ Hive客户端已在新窗口中启动" -ForegroundColor Green
        Write-Host "提示：关闭新窗口可退出Hive" -ForegroundColor Gray
    }
    catch {
        Write-Host "❌ 启动Hive客户端失败: $($_.Exception.Message)" -ForegroundColor Red
    }
}

try {
    Write-Host "`n==================== Hive 启动工具 ====================" -ForegroundColor Cyan
    
    # 读取配置文件
    $configPath = ".\config\config.ini"
    Write-Host "正在读取配置文件: $configPath" -ForegroundColor Yellow
    
    $config = Read-IniFile -FilePath $configPath
    
    # 设置环境变量
    Write-Host "`n设置Hive环境变量..." -ForegroundColor Yellow
    $envValid = Set-HiveEnvironment -Config $config
    
    if (-not $envValid) {
        Write-Host "`n⚠️ 检测到环境变量配置问题" -ForegroundColor Yellow
        Write-Host "是否继续? (y/n): " -ForegroundColor Yellow -NoNewline
        $continue = Read-Host
        if ($continue -ne "y" -and $continue -ne "Y") {
            Write-Host "已取消" -ForegroundColor Gray
            exit
        }
    }
    
    # 检查Hadoop DFS状态
    Write-Host "`n检查Hadoop DFS状态..." -ForegroundColor Yellow
    $hadoopRunning = Test-HadoopDFS
    
    if (-not $hadoopRunning) {
        Write-Host "`n⚠️ Hadoop DFS 未运行或未完全启动" -ForegroundColor Yellow
        Write-Host "Hive 依赖 Hadoop DFS，建议先启动 Hadoop" -ForegroundColor Yellow
        Write-Host "提示：运行 .\start-env-admin.ps1 启动Hadoop DFS" -ForegroundColor Cyan
        Write-Host "`n是否仍要继续启动Hive? (y/n): " -ForegroundColor Yellow -NoNewline
        $continue = Read-Host
        if ($continue -ne "y" -and $continue -ne "Y") {
            Write-Host "已取消" -ForegroundColor Gray
            exit
        }
    }
    
    Write-Host "`n==================================================`n" -ForegroundColor Cyan
    
    # 显示当前环境
    Write-Host "当前环境变量:" -ForegroundColor Cyan
    Write-Host "  HIVE_HOME: $env:HIVE_HOME" -ForegroundColor White
    Write-Host "  HADOOP_HOME: $env:HADOOP_HOME" -ForegroundColor White
    Write-Host "  JAVA_HOME: $env:JAVA_HOME" -ForegroundColor White
    
    Write-Host "`nHive工具已就绪！" -ForegroundColor Green
}
catch {
    Write-Host "执行过程中出现错误: $($_.Exception.Message)" -ForegroundColor Red
    Write-Host "错误详情: $($_.ScriptStackTrace)" -ForegroundColor Red
}

# 用户选择操作
do {
    Write-Host "`n请选择操作:" -ForegroundColor Yellow
    Write-Host "1. 启动Hive客户端（新窗口）" -ForegroundColor White
    Write-Host "2. 检查Hadoop状态" -ForegroundColor White
    Write-Host "3. 显示环境变量" -ForegroundColor White
    Write-Host "4. 测试Hive配置" -ForegroundColor White
    Write-Host "exit. 退出" -ForegroundColor White
    Write-Host "请输入选择 (1/2/3/4/exit): " -ForegroundColor Cyan -NoNewline

    $choice = Read-Host
    
    switch ($choice) {
        "1" {
            Start-HiveClient
        }
        "2" {
            Write-Host "`n检查Hadoop DFS状态..." -ForegroundColor Yellow
            $hadoopStatus = Test-HadoopDFS
            
            if ($hadoopStatus) {
                Write-Host "Hadoop DFS 运行正常" -ForegroundColor Green
            }
            else {
                Write-Host "Hadoop DFS 未运行或状态异常" -ForegroundColor Red
                Write-Host "请运行 .\start-env-admin.ps1 启动Hadoop" -ForegroundColor Yellow
            }
        }
        "3" {
            Write-Host "`n当前环境变量:" -ForegroundColor Cyan
            Write-Host "  HIVE_HOME: $env:HIVE_HOME" -ForegroundColor White
            Write-Host "  HADOOP_HOME: $env:HADOOP_HOME" -ForegroundColor White
            Write-Host "  JAVA_HOME: $env:JAVA_HOME" -ForegroundColor White
            Write-Host "  PATH (前200字符): $($env:Path.Substring(0, [Math]::Min(200, $env:Path.Length)))..." -ForegroundColor White
            
            # 显示路径是否存在
            Write-Host "`n路径验证:" -ForegroundColor Cyan
            @{
                "HIVE_HOME" = $env:HIVE_HOME
                "HADOOP_HOME" = $env:HADOOP_HOME
                "JAVA_HOME" = $env:JAVA_HOME
            }.GetEnumerator() | ForEach-Object {
                $status = if (Test-Path $_.Value) { "✓" } else { "❌" }
                $color = if (Test-Path $_.Value) { "Green" } else { "Red" }
                Write-Host "  $status $($_.Key): $($_.Value)" -ForegroundColor $color
            }
        }
        "4" {
            Write-Host "`n测试Hive配置..." -ForegroundColor Yellow
            
            # 检查Hive配置文件
            $hiveConfDir = "$env:HIVE_HOME\conf"
            $hiveSiteXml = "$hiveConfDir\hive-site.xml"
            
            if (Test-Path $hiveSiteXml) {
                Write-Host "  ✓ hive-site.xml 存在" -ForegroundColor Green
                
                # 读取配置文件检查关键配置
                $xmlContent = Get-Content $hiveSiteXml -Raw
                if ($xmlContent -match "javax.jdo.option.ConnectionURL") {
                    Write-Host "  ✓ MySQL连接配置已设置" -ForegroundColor Green
                }
                else {
                    Write-Host "  ⚠️ MySQL连接配置未找到" -ForegroundColor Yellow
                }
            }
            else {
                Write-Host "  ❌ hive-site.xml 不存在: $hiveSiteXml" -ForegroundColor Red
            }
            
            # 检查MySQL JDBC驱动
            $jdbcJar = "$env:HIVE_HOME\lib\mysql-connector-j-8.0.33.jar"
            if (Test-Path $jdbcJar) {
                Write-Host "  ✓ MySQL JDBC驱动存在" -ForegroundColor Green
            }
            else {
                Write-Host "  ⚠️ MySQL JDBC驱动未找到: $jdbcJar" -ForegroundColor Yellow
            }
            
            # 检查Hive可执行文件
            $hiveCmd = "$env:HIVE_HOME\bin\hive.cmd"
            if (Test-Path $hiveCmd) {
                Write-Host "  ✓ hive.cmd 存在" -ForegroundColor Green
            }
            else {
                Write-Host "  ❌ hive.cmd 不存在: $hiveCmd" -ForegroundColor Red
            }
        }
        "exit" {
            Write-Host "正在退出..." -ForegroundColor Gray
            break
        }
        default {
            Write-Host "无效选择，请输入1、2、3、4或exit" -ForegroundColor Red
        }
    }
} while ($choice -ne "exit")

Write-Host "`n感谢使用！" -ForegroundColor Green
