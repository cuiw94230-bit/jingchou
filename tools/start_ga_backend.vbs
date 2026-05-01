Option Explicit

Dim shell, fso, projectTools, pythonExe, scriptPath, command, netstatCheck, netstatOutput
Set shell = CreateObject("WScript.Shell")
Set fso = CreateObject("Scripting.FileSystemObject")

projectTools = fso.GetParentFolderName(WScript.ScriptFullName)
pythonExe = "C:\Users\laoj0\AppData\Local\Programs\Python\Python314\python.exe"
scriptPath = projectTools & "\ga_backend_server.py"
Set netstatCheck = shell.Exec("cmd /c netstat -ano | findstr /R "":8765 .*LISTENING""")
netstatOutput = netstatCheck.StdOut.ReadAll()

If Not fso.FileExists(pythonExe) Then
  MsgBox "Python not found: " & pythonExe, 16, "启动GA后台"
  WScript.Quit 1
End If

If Not fso.FileExists(scriptPath) Then
  MsgBox "GA backend script not found: " & scriptPath, 16, "启动GA后台"
  WScript.Quit 1
End If

If Len(Trim(netstatOutput)) > 0 Then
  MsgBox "GA 后台端口 8765 已被占用。请先关闭旧的后台，或先验证 http://127.0.0.1:8765/health 。", 48, "启动GA后台"
  WScript.Quit 1
End If

command = Chr(34) & pythonExe & Chr(34) & " " & Chr(34) & scriptPath & Chr(34)
shell.Run command, 1, False
